import Database from 'better-sqlite3';
import path from 'node:path';
import { mkdir } from 'node:fs/promises';
import type { BatchJobFileResult, NormalizationConfig, StorageDriver } from '../types.js';
import type { RetentionPolicy } from './retention.js';

type RawResultRow = BatchJobFileResult & {
  opts: string | null;
  normalization?: string | null;
  audioSampleRateHz?: number | null;
  audioChannels?: number | null;
  audioEncoding?: string | null;
};

function mapResultRow(row: RawResultRow): BatchJobFileResult {
  const {
    normalization,
    audioSampleRateHz,
    audioChannels,
    audioEncoding,
    ...rest
  } = row;
  return {
    ...rest,
    degraded: row.degraded == null ? undefined : Boolean(row.degraded),
    opts: row.opts ? (JSON.parse(row.opts) as Record<string, unknown>) : undefined,
    normalizationUsed: normalization ? (JSON.parse(normalization) as NormalizationConfig) : undefined,
    audioSpec:
      typeof audioSampleRateHz === 'number' &&
      Number.isFinite(audioSampleRateHz) &&
      typeof audioChannels === 'number' &&
      Number.isFinite(audioChannels) &&
      typeof audioEncoding === 'string' &&
      audioEncoding.length > 0
        ? {
            sampleRateHz: audioSampleRateHz,
            channels: audioChannels,
            encoding: audioEncoding as 'linear16',
          }
        : undefined,
  };
}

export class SqliteStore implements StorageDriver<BatchJobFileResult> {
  private db: Database.Database | null = null;
  private readonly retentionMs: number | undefined;
  private readonly maxRows: number | undefined;
  private readonly pruneIntervalMs: number;
  private lastPruned = 0;

  constructor(
    private readonly dbPath: string,
    existingDb?: Database.Database,
    retention?: RetentionPolicy
  ) {
    this.db = existingDb ?? null;
    this.retentionMs = retention?.retentionMs;
    this.maxRows = retention?.maxRows;
    this.pruneIntervalMs = retention?.pruneIntervalMs ?? 5 * 60 * 1000;
  }

  async init(): Promise<void> {
    await mkdir(path.dirname(this.dbPath), { recursive: true });
    if (!this.db) {
      this.db = new Database(this.dbPath);
      this.db.pragma('journal_mode = WAL');
      this.db.pragma('busy_timeout = 1000');
    }
    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS results (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          jobId TEXT,
          path TEXT,
          provider TEXT,
          lang TEXT,
          durationSec REAL,
          processingTimeMs INTEGER,
          rtf REAL,
          cer REAL,
          wer REAL,
          latencyMs INTEGER,
          vendorProcessingMs INTEGER,
          audioSampleRateHz INTEGER,
          audioChannels INTEGER,
          audioEncoding TEXT,
          degraded INTEGER,
          normalization TEXT,
          text TEXT,
          refText TEXT,
          opts TEXT,
          createdAt TEXT
        )`
      )
      .run();

    // simple migration: add jobId if missing
    const jobColumns = this.db.prepare(`PRAGMA table_info('results')`).all() as Database.ColumnDefinition[];
    const hasJobId = jobColumns.some((row) => row.name === 'jobId');
    if (!hasJobId) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN jobId TEXT`).run();
    }
    const hasVendorProcessing = jobColumns.some((row) => row.name === 'vendorProcessingMs');
    if (!hasVendorProcessing) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN vendorProcessingMs INTEGER`).run();
    }
    const hasDegraded = jobColumns.some((row) => row.name === 'degraded');
    if (!hasDegraded) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN degraded INTEGER`).run();
    }
    const hasNormalization = jobColumns.some((row) => row.name === 'normalization');
    if (!hasNormalization) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN normalization TEXT`).run();
    }
    const hasAudioSampleRate = jobColumns.some((row) => row.name === 'audioSampleRateHz');
    if (!hasAudioSampleRate) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN audioSampleRateHz INTEGER`).run();
    }
    const hasAudioChannels = jobColumns.some((row) => row.name === 'audioChannels');
    if (!hasAudioChannels) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN audioChannels INTEGER`).run();
    }
    const hasAudioEncoding = jobColumns.some((row) => row.name === 'audioEncoding');
    if (!hasAudioEncoding) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN audioEncoding TEXT`).run();
    }

    const createdAtColumns = this.db.prepare(`PRAGMA table_info('results')`).all() as Database.ColumnDefinition[];
    const hasCreatedAt = createdAtColumns.some((row) => row.name === 'createdAt');
    if (!hasCreatedAt) {
      this.db.prepare(`ALTER TABLE results ADD COLUMN createdAt TEXT`).run();
      this.db.prepare(`UPDATE results SET createdAt = datetime('now') WHERE createdAt IS NULL`).run();
    }
  }

  async append(record: BatchJobFileResult): Promise<void> {
    if (!this.db) throw new Error('SQLite store not initialized');
    this.db
      .prepare(
        `INSERT INTO results (jobId, path, provider, lang, durationSec, processingTimeMs, rtf, cer, wer, latencyMs, vendorProcessingMs, audioSampleRateHz, audioChannels, audioEncoding, degraded, normalization, text, refText, opts, createdAt)
         VALUES (@jobId, @path, @provider, @lang, @durationSec, @processingTimeMs, @rtf, @cer, @wer, @latencyMs, @vendorProcessingMs, @audioSampleRateHz, @audioChannels, @audioEncoding, @degraded, @normalization, @text, @refText, @opts, @createdAt)`
      )
      .run({
        ...record,
        audioSampleRateHz: record.audioSpec?.sampleRateHz ?? null,
        audioChannels: record.audioSpec?.channels ?? null,
        audioEncoding: record.audioSpec?.encoding ?? null,
        opts: record.opts ? JSON.stringify(record.opts) : null,
        normalization: record.normalizationUsed ? JSON.stringify(record.normalizationUsed) : null,
        createdAt: record.createdAt ?? new Date().toISOString(),
      });

    await this.maybePrune();
  }

  async readAll(): Promise<BatchJobFileResult[]> {
    if (!this.db) throw new Error('SQLite store not initialized');
    const rows = this.db.prepare('SELECT * FROM results ORDER BY id DESC').all() as RawResultRow[];
    return rows.map(mapResultRow);
  }

  async readRecent(limit: number): Promise<BatchJobFileResult[]> {
    if (!this.db) throw new Error('SQLite store not initialized');
    const rows = this.db.prepare('SELECT * FROM results ORDER BY id DESC LIMIT ?').all(limit) as RawResultRow[];
    return rows.map(mapResultRow);
  }

  async readByJob(jobId: string): Promise<BatchJobFileResult[]> {
    if (!this.db) throw new Error('SQLite store not initialized');
    const rows = this.db
      .prepare('SELECT * FROM results WHERE jobId = ? ORDER BY id DESC')
      .all(jobId) as RawResultRow[];
    return rows.map(mapResultRow);
  }

  async listJobIds(): Promise<string[]> {
    if (!this.db) throw new Error('SQLite store not initialized');
    const rows = this.db
      .prepare(
        `SELECT jobId
         FROM results
         WHERE jobId IS NOT NULL AND jobId != ''
         GROUP BY jobId
         ORDER BY MAX(id) DESC`
      )
      .all() as { jobId: string }[];
    return rows.map((row) => row.jobId);
  }

  private async maybePrune(): Promise<void> {
    if (!this.db) throw new Error('SQLite store not initialized');
    if (!this.retentionMs && !this.maxRows) return;
    const now = Date.now();
    if (now - this.lastPruned < this.pruneIntervalMs) return;
    this.lastPruned = now;

    const trx = this.db.transaction(() => {
      if (this.retentionMs) {
        const cutoffIso = new Date(now - this.retentionMs).toISOString();
        this.db!
          .prepare(`DELETE FROM results WHERE createdAt IS NOT NULL AND createdAt < ?`)
          .run(cutoffIso);
      }
      if (this.maxRows) {
        this.db!
          .prepare(
            `DELETE FROM results WHERE id NOT IN (SELECT id FROM results ORDER BY id DESC LIMIT ?)`
          )
          .run(this.maxRows);
      }
    });
    trx();
  }
}
