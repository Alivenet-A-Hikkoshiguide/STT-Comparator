import { createReadStream } from 'node:fs';
import { appendFile, mkdir, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';
import type { StorageDriver } from '../types.js';
import type { RetentionPolicy } from './retention.js';
import { logger } from '../logger.js';

export class JsonlStore<T> implements StorageDriver<T> {
  private readonly retentionMs: number | undefined;
  private readonly maxRows: number | undefined;
  private readonly pruneIntervalMs: number;
  private lastPruned = 0;
  private pruneInFlight: Promise<void> | null = null;
  private writeChain: Promise<void> = Promise.resolve();

  constructor(private filepath: string, retention?: RetentionPolicy) {
    this.retentionMs = retention?.retentionMs;
    this.maxRows = retention?.maxRows;
    this.pruneIntervalMs = retention?.pruneIntervalMs ?? 5 * 60 * 1000; // 5 minutes
  }

  async init(): Promise<void> {
    await mkdir(path.dirname(this.filepath), { recursive: true });
  }

  async append(record: T): Promise<void> {
    await this.enqueueWrite(async () => {
      await appendFile(this.filepath, `${JSON.stringify(record)}\n`);
      this.schedulePrune(record);
    });
  }

  async readAll(): Promise<T[]> {
    await this.waitForWrites();
    return this.readAllInternal();
  }

  private async readAllInternal(): Promise<T[]> {
    const records: T[] = [];
    await this.forEachRecord((record) => {
      records.push(record);
    });
    return records;
  }

  async readRecent(limit: number): Promise<T[]> {
    await this.waitForWrites();
    if (limit <= 0) {
      return this.readAll();
    }
    const ring: T[] = [];
    await this.forEachRecord((record) => {
      if (ring.length >= limit) {
        ring.shift();
      }
      ring.push(record);
    });
    return ring.reverse();
  }

  async readByJob(jobId: string): Promise<T[]> {
    await this.waitForWrites();
    const rows: T[] = [];
    await this.forEachRecord((record) => {
      const candidate = record as { jobId?: string };
      if (candidate.jobId === jobId) {
        rows.push(record);
      }
    });
    return rows.reverse();
  }

  async listJobIds(): Promise<string[]> {
    await this.waitForWrites();
    const ids = new Set<string>();
    await this.forEachRecord((record) => {
      const candidate = record as { jobId?: string };
      const jobId = candidate.jobId;
      if (typeof jobId === 'string' && jobId.length > 0) {
        ids.add(jobId);
      }
    });
    return [...ids];
  }

  private schedulePrune(_sampleRecord?: T): void {
    if (!this.retentionMs && !this.maxRows) return;
    if (this.pruneInFlight) return;
    const now = Date.now();
    if (now - this.lastPruned < this.pruneIntervalMs) return;
    this.lastPruned = now;

    this.pruneInFlight = this.enqueueWrite(() => this.runPrune()).finally(() => {
      this.pruneInFlight = null;
    });
  }

  private async runPrune(): Promise<void> {
    try {
      await this.maybePrune();
    } catch (error) {
      logger.warn({
        event: 'jsonl_prune_failed',
        filepath: this.filepath,
        message: error instanceof Error ? error.message : 'unknown prune error',
      });
    }
  }

  private async maybePrune(): Promise<void> {
    const now = Date.now();
    const all = await this.readAllInternal();
    if (all.length === 0) return;

    const cutoff = this.retentionMs ? new Date(now - this.retentionMs) : null;

    const filtered = all
      .map((row) => ({ row, ts: this.extractTimestamp(row) }))
      .filter(({ ts }) => {
        if (!cutoff) return true;
        return ts ? ts >= cutoff.getTime() : false;
      })
      .sort((a, b) => (a.ts ?? 0) - (b.ts ?? 0))
      .map(({ row }) => row);

    const pruned = this.maxRows ? filtered.slice(-this.maxRows) : filtered;

    // Only rewrite when something changed
    if (pruned.length !== all.length) {
      const tmpPath = `${this.filepath}.${randomUUID()}.tmp`;
      const payload = pruned.map((r) => JSON.stringify(r)).join('\n') + '\n';
      await writeFile(tmpPath, payload, 'utf-8');
      await rename(tmpPath, this.filepath);
    }
  }

  private enqueueWrite(task: () => Promise<void>): Promise<void> {
    const operation = this.writeChain.then(task, task);
    this.writeChain = operation.catch(() => undefined);
    return operation;
  }

  private async waitForWrites(): Promise<void> {
    await this.writeChain;
    if (this.pruneInFlight) {
      await this.pruneInFlight;
    }
  }

  private extractTimestamp(row: T): number | null {
    const candidateRecord = row as {
      createdAt?: string;
      endedAt?: string;
      startedAt?: string;
      recordedAt?: string;
    };
    const candidate =
      candidateRecord.createdAt ??
      candidateRecord.endedAt ??
      candidateRecord.startedAt ??
      candidateRecord.recordedAt;
    const ts = candidate ? Date.parse(candidate as string) : NaN;
    return Number.isFinite(ts) ? ts : null;
  }

  private parseLine(line: string): T {
    try {
      return JSON.parse(line) as T;
    } catch (error) {
      const message = error instanceof Error ? error.message : 'failed to parse JSONL line';
      throw new Error(`invalid JSONL row: ${message}`);
    }
  }

  private async forEachRecord(visitor: (record: T) => void): Promise<void> {
    try {
      const stream = createReadStream(this.filepath, { encoding: 'utf-8' });
      const rl = createInterface({
        input: stream,
        crlfDelay: Infinity,
      });
      for await (const line of rl) {
        if (!line) continue;
        visitor(this.parseLine(line));
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return;
      }
      throw error;
    }
  }
}
