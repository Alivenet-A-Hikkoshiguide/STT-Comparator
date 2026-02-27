import type { StorageDriver, BatchJobFileResult } from '../types.js';
import { summarizeJob, summarizeJobByProvider } from '../utils/summary.js';
import type { JobSummary } from '../utils/summary.js';

export interface JobHistoryEntry {
  jobId: string;
  provider: string;
  providers: string[];
  lang: string;
  createdAt: string;
  updatedAt: string;
  total: number;
  summary: JobSummary;
  summaryByProvider?: Record<string, JobSummary>;
}

interface JobHistoryOptions {
  syncIntervalMs?: number;
}

export class JobHistory {
  private readonly rowsByJob = new Map<string, BatchJobFileResult[]>();
  private readonly entriesByJob = new Map<string, JobHistoryEntry>();
  private readonly syncIntervalMs: number;
  private lastSyncAt = 0;

  constructor(
    private readonly storage: StorageDriver<BatchJobFileResult>,
    options?: JobHistoryOptions
  ) {
    this.syncIntervalMs = options?.syncIntervalMs ?? 30_000;
  }

  async init(): Promise<void> {
    await this.storage.init();
    await this.rebuild();
  }

  recordRow(record: BatchJobFileResult): void {
    if (!record.jobId) return;
    const bucket = this.rowsByJob.get(record.jobId) ?? [];
    bucket.push(record);
    this.rowsByJob.set(record.jobId, bucket);
    this.upsertEntry(record.jobId, bucket);
  }

  async list(): Promise<JobHistoryEntry[]> {
    await this.syncWithStorage();
    return [...this.entriesByJob.values()]
      .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime());
  }

  get(jobId: string): BatchJobFileResult[] | undefined {
    return this.rowsByJob.get(jobId);
  }

  private buildEntry(jobId: string, rows: BatchJobFileResult[]): JobHistoryEntry | null {
    if (rows.length === 0) return null;
    const summary = summarizeJob(rows);
    const summaryByProvider = summarizeJobByProvider(rows);
    const providers = Array.from(new Set(rows.map((row) => row.provider)));
    const provider = providers[0];
    const lang = rows[0].lang;
    const createdAt = this.getEarliest(rows)?.createdAt ?? new Date().toISOString();
    const updatedAt = this.getLatest(rows)?.createdAt ?? createdAt;
    return {
      jobId,
      provider,
      providers,
      lang,
      createdAt,
      updatedAt,
      total: rows.length,
      summary,
      summaryByProvider,
    };
  }

  private getEarliest(rows: BatchJobFileResult[]): BatchJobFileResult | null {
    return rows.reduce<BatchJobFileResult | null>((prev, current) => {
      if (!prev) return current;
      const prevTs = Date.parse(prev.createdAt ?? '');
      const currentTs = Date.parse(current.createdAt ?? '');
      return Number.isFinite(currentTs) && currentTs < (Number.isFinite(prevTs) ? prevTs : Infinity) ? current : prev;
    }, null);
  }

  private getLatest(rows: BatchJobFileResult[]): BatchJobFileResult | null {
    return rows.reduce<BatchJobFileResult | null>((prev, current) => {
      if (!prev) return current;
      const prevTs = Date.parse(prev.createdAt ?? '');
      const currentTs = Date.parse(current.createdAt ?? '');
      return Number.isFinite(currentTs) && currentTs > (Number.isFinite(prevTs) ? prevTs : -Infinity) ? current : prev;
    }, null);
  }

  private mapByJob(rows: BatchJobFileResult[]): Map<string, BatchJobFileResult[]> {
    const result = new Map<string, BatchJobFileResult[]>();
    for (const row of rows) {
      if (!row.jobId) continue;
      const bucket = result.get(row.jobId);
      if (bucket) {
        bucket.push(row);
      } else {
        result.set(row.jobId, [row]);
      }
    }
    return result;
  }

  private rebuildFrom(rows: BatchJobFileResult[]): void {
    this.rowsByJob.clear();
    this.entriesByJob.clear();
    for (const [jobId, records] of this.mapByJob(rows)) {
      this.rowsByJob.set(jobId, records);
      this.upsertEntry(jobId, records);
    }
  }

  private async rebuild(): Promise<void> {
    const all = await this.storage.readAll();
    this.rebuildFrom(all);
    this.lastSyncAt = Date.now();
  }

  private async syncWithStorage(): Promise<void> {
    const now = Date.now();
    if (now - this.lastSyncAt < this.syncIntervalMs) {
      return;
    }

    if (typeof this.storage.listJobIds === 'function' && typeof this.storage.readByJob === 'function') {
      const ids = new Set((await this.storage.listJobIds()).filter((id) => id.length > 0));
      for (const jobId of [...this.rowsByJob.keys()]) {
        if (!ids.has(jobId)) {
          this.rowsByJob.delete(jobId);
          this.entriesByJob.delete(jobId);
        }
      }
      for (const jobId of ids) {
        if (this.rowsByJob.has(jobId)) {
          continue;
        }
        const rows = await this.storage.readByJob(jobId);
        if (rows.length === 0) {
          continue;
        }
        this.rowsByJob.set(jobId, rows);
        this.upsertEntry(jobId, rows);
      }
      this.lastSyncAt = now;
      return;
    }

    const currentRows = await this.storage.readAll();
    this.rebuildFrom(currentRows);
    this.lastSyncAt = now;
  }

  private upsertEntry(jobId: string, rows: BatchJobFileResult[]): void {
    const entry = this.buildEntry(jobId, rows);
    if (!entry) {
      this.entriesByJob.delete(jobId);
      return;
    }
    this.entriesByJob.set(jobId, entry);
  }
}
