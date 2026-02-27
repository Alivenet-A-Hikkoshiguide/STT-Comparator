import Database from 'better-sqlite3';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import type {
  EvaluationManifest,
  JobWarning,
  ProviderId,
  TranscriptionOptions,
} from '../types.js';

export type PersistedJobStatus = 'queued' | 'running' | 'completed' | 'failed';
export type PersistedTaskStatus = 'pending' | 'running' | 'done' | 'failed';

export interface PersistedBatchFile {
  index: number;
  originalname: string;
  path: string;
  size?: number;
}

export interface PersistedBatchTask {
  fileIndex: number;
  provider: ProviderId;
  status: PersistedTaskStatus;
  attempts: number;
  lastError?: string;
}

export interface PersistedBatchJob {
  jobId: string;
  providers: ProviderId[];
  lang: string;
  total: number;
  done: number;
  failed: number;
  status: PersistedJobStatus;
  errors: { file: string; message: string }[];
  warnings: JobWarning[];
  manifest?: EvaluationManifest;
  options?: TranscriptionOptions;
  files: PersistedBatchFile[];
  tasks: PersistedBatchTask[];
  createdAt: string;
  updatedAt: string;
}

export interface PersistedBatchJobSummary {
  jobId: string;
  providers: ProviderId[];
  lang: string;
  total: number;
  done: number;
  failed: number;
  status: PersistedJobStatus;
  errors: { file: string; message: string }[];
  warnings: JobWarning[];
  createdAt: string;
  updatedAt: string;
}

interface PersistedBatchJobRow {
  jobId: string;
  providersJson: string;
  lang: string;
  total: number;
  done: number;
  failed: number;
  status: PersistedJobStatus;
  errorsJson: string;
  warningsJson: string;
  manifestJson: string | null;
  optionsJson: string | null;
  createdAt: string;
  updatedAt: string;
}

function parseJsonOr<T>(value: string | null | undefined, fallback: T): T {
  if (!value) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function nowIso(): string {
  return new Date().toISOString();
}

export class PersistentBatchJobStore {
  private db: Database.Database | null = null;

  constructor(
    private readonly dbPath: string,
    existingDb?: Database.Database
  ) {
    this.db = existingDb ?? null;
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
        `CREATE TABLE IF NOT EXISTS jobs (
          jobId TEXT PRIMARY KEY,
          providersJson TEXT NOT NULL,
          lang TEXT NOT NULL,
          total INTEGER NOT NULL,
          done INTEGER NOT NULL DEFAULT 0,
          failed INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL,
          errorsJson TEXT NOT NULL DEFAULT '[]',
          warningsJson TEXT NOT NULL DEFAULT '[]',
          manifestJson TEXT,
          optionsJson TEXT,
          createdAt TEXT NOT NULL,
          updatedAt TEXT NOT NULL
        )`
      )
      .run();

    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS job_files (
          jobId TEXT NOT NULL,
          fileIndex INTEGER NOT NULL,
          originalname TEXT NOT NULL,
          path TEXT NOT NULL,
          size INTEGER,
          PRIMARY KEY (jobId, fileIndex),
          FOREIGN KEY (jobId) REFERENCES jobs(jobId) ON DELETE CASCADE
        )`
      )
      .run();

    this.db
      .prepare(
        `CREATE TABLE IF NOT EXISTS job_tasks (
          jobId TEXT NOT NULL,
          fileIndex INTEGER NOT NULL,
          provider TEXT NOT NULL,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          lastError TEXT,
          updatedAt TEXT NOT NULL,
          PRIMARY KEY (jobId, fileIndex, provider),
          FOREIGN KEY (jobId, fileIndex) REFERENCES job_files(jobId, fileIndex) ON DELETE CASCADE
        )`
      )
      .run();

    this.db.prepare(`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)`).run();
    this.db.prepare(`CREATE INDEX IF NOT EXISTS idx_tasks_job_status ON job_tasks(jobId, status)`).run();
    const jobColumns = this.db.prepare(`PRAGMA table_info(jobs)`).all() as Array<{ name: string }>;
    const hasWarnings = jobColumns.some((column) => column.name === 'warningsJson');
    if (!hasWarnings) {
      this.db
        .prepare(`ALTER TABLE jobs ADD COLUMN warningsJson TEXT NOT NULL DEFAULT '[]'`)
        .run();
    }
  }

  async createJob(input: {
    jobId: string;
    providers: ProviderId[];
    lang: string;
    files: PersistedBatchFile[];
    manifest?: EvaluationManifest;
    options?: TranscriptionOptions;
  }): Promise<void> {
    const db = this.requireDb();
    const now = nowIso();
    const total = input.files.length * input.providers.length;
    const tx = db.transaction(() => {
      db.prepare(
        `INSERT INTO jobs (
          jobId, providersJson, lang, total, done, failed, status, errorsJson, warningsJson, manifestJson, optionsJson, createdAt, updatedAt
        ) VALUES (
          @jobId, @providersJson, @lang, @total, 0, 0, 'queued', '[]', '[]', @manifestJson, @optionsJson, @createdAt, @updatedAt
        )`
      ).run({
        jobId: input.jobId,
        providersJson: JSON.stringify(input.providers),
        lang: input.lang,
        total,
        manifestJson: input.manifest ? JSON.stringify(input.manifest) : null,
        optionsJson: input.options ? JSON.stringify(input.options) : null,
        createdAt: now,
        updatedAt: now,
      });

      const insertFile = db.prepare(
        `INSERT INTO job_files (jobId, fileIndex, originalname, path, size)
         VALUES (@jobId, @fileIndex, @originalname, @path, @size)`
      );
      for (const file of input.files) {
        insertFile.run({
          jobId: input.jobId,
          fileIndex: file.index,
          originalname: file.originalname,
          path: file.path,
          size: file.size ?? null,
        });
      }

      const insertTask = db.prepare(
        `INSERT INTO job_tasks (jobId, fileIndex, provider, status, attempts, lastError, updatedAt)
         VALUES (@jobId, @fileIndex, @provider, 'pending', 0, NULL, @updatedAt)`
      );
      for (const file of input.files) {
        for (const provider of input.providers) {
          insertTask.run({
            jobId: input.jobId,
            fileIndex: file.index,
            provider,
            updatedAt: now,
          });
        }
      }
    });
    tx();
  }

  async setJobStatus(jobId: string, status: PersistedJobStatus): Promise<void> {
    const db = this.requireDb();
    db.prepare(`UPDATE jobs SET status = ?, updatedAt = ? WHERE jobId = ?`).run(status, nowIso(), jobId);
  }

  async resetRunningTasks(jobId: string): Promise<void> {
    const db = this.requireDb();
    db
      .prepare(`UPDATE job_tasks SET status = 'pending', updatedAt = ? WHERE jobId = ? AND status = 'running'`)
      .run(nowIso(), jobId);
  }

  async markTaskRunning(jobId: string, fileIndex: number, provider: ProviderId): Promise<void> {
    const db = this.requireDb();
    db
      .prepare(
        `UPDATE job_tasks
         SET status = 'running', attempts = attempts + 1, updatedAt = ?
         WHERE jobId = ? AND fileIndex = ? AND provider = ? AND status = 'pending'`
      )
      .run(nowIso(), jobId, fileIndex, provider);
  }

  async markTaskDone(jobId: string, fileIndex: number, provider: ProviderId): Promise<void> {
    const db = this.requireDb();
    const now = nowIso();
    const tx = db.transaction(() => {
      const updated = db
        .prepare(
          `UPDATE job_tasks
           SET status = 'done', updatedAt = ?, lastError = NULL
           WHERE jobId = ? AND fileIndex = ? AND provider = ? AND status IN ('pending', 'running')`
        )
        .run(now, jobId, fileIndex, provider);
      if (updated.changes > 0) {
        db.prepare(`UPDATE jobs SET done = done + 1, updatedAt = ? WHERE jobId = ?`).run(now, jobId);
      }
    });
    tx();
  }

  async markTaskFailed(
    jobId: string,
    fileIndex: number,
    provider: ProviderId,
    error: { file: string; message: string }
  ): Promise<void> {
    const db = this.requireDb();
    const now = nowIso();
    const tx = db.transaction(() => {
      const updated = db
        .prepare(
          `UPDATE job_tasks
           SET status = 'failed', updatedAt = ?, lastError = ?
           WHERE jobId = ? AND fileIndex = ? AND provider = ? AND status IN ('pending', 'running')`
        )
        .run(now, error.message, jobId, fileIndex, provider);
      if (updated.changes === 0) {
        return;
      }

      const row = db
        .prepare(`SELECT errorsJson FROM jobs WHERE jobId = ?`)
        .get(jobId) as { errorsJson: string } | undefined;
      const errors = parseJsonOr<{ file: string; message: string }[]>(row?.errorsJson, []);
      errors.push(error);
      db
        .prepare(`UPDATE jobs SET failed = failed + 1, errorsJson = ?, updatedAt = ? WHERE jobId = ?`)
        .run(JSON.stringify(errors), now, jobId);
    });
    tx();
  }

  async addJobWarning(jobId: string, warning: JobWarning): Promise<void> {
    const db = this.requireDb();
    const now = nowIso();
    const tx = db.transaction(() => {
      const row = db
        .prepare(`SELECT warningsJson FROM jobs WHERE jobId = ?`)
        .get(jobId) as { warningsJson: string } | undefined;
      const warnings = parseJsonOr<JobWarning[]>(row?.warningsJson, []);
      if (warnings.some((entry) => entry.code === warning.code)) {
        return;
      }
      warnings.push(warning);
      db
        .prepare(`UPDATE jobs SET warningsJson = ?, updatedAt = ? WHERE jobId = ?`)
        .run(JSON.stringify(warnings), now, jobId);
    });
    tx();
  }

  async getJob(jobId: string): Promise<PersistedBatchJob | null> {
    const db = this.requireDb();
    const row = db
      .prepare(`SELECT * FROM jobs WHERE jobId = ?`)
      .get(jobId) as PersistedBatchJobRow | undefined;
    if (!row) return null;
    const files = db
      .prepare(`SELECT * FROM job_files WHERE jobId = ? ORDER BY fileIndex ASC`)
      .all(jobId) as Array<{
      jobId: string;
      fileIndex: number;
      originalname: string;
      path: string;
      size: number | null;
    }>;
    const tasks = db
      .prepare(`SELECT * FROM job_tasks WHERE jobId = ? ORDER BY fileIndex ASC, provider ASC`)
      .all(jobId) as Array<{
      jobId: string;
      fileIndex: number;
      provider: ProviderId;
      status: PersistedTaskStatus;
      attempts: number;
      lastError: string | null;
    }>;
    return this.composeJob(row, files, tasks);
  }

  async getJobStatus(jobId: string): Promise<{
    jobId: string;
    total: number;
    done: number;
    failed: number;
    errors: { file: string; message: string }[];
    warnings: JobWarning[];
    providers: ProviderId[];
    status: PersistedJobStatus;
  } | null> {
    const db = this.requireDb();
    const row = db
      .prepare(
        `SELECT jobId, total, done, failed, errorsJson, warningsJson, providersJson, status FROM jobs WHERE jobId = ?`
      )
      .get(jobId) as
      | {
          jobId: string;
          total: number;
          done: number;
          failed: number;
          errorsJson: string;
          warningsJson: string;
          providersJson: string;
          status: PersistedJobStatus;
        }
      | undefined;
    if (!row) return null;
    return this.composeJobSummary(row);
  }

  async listJobs(limit = 200): Promise<PersistedBatchJobSummary[]> {
    const db = this.requireDb();
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(1000, Math.floor(limit))) : 200;
    const rows = db
      .prepare(
        `SELECT jobId, providersJson, lang, total, done, failed, status, errorsJson, warningsJson, createdAt, updatedAt
         FROM jobs
         ORDER BY updatedAt DESC
         LIMIT ?`
      )
      .all(safeLimit) as Array<
      Pick<
        PersistedBatchJobRow,
        'jobId' | 'providersJson' | 'lang' | 'total' | 'done' | 'failed' | 'status' | 'errorsJson' | 'warningsJson' | 'createdAt' | 'updatedAt'
      >
    >;
    return rows.map((row) => this.composeJobSummary(row));
  }

  async listActiveJobs(): Promise<PersistedBatchJob[]> {
    const db = this.requireDb();
    const rows = db
      .prepare(`SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY createdAt ASC`)
      .all() as PersistedBatchJobRow[];
    const jobs: PersistedBatchJob[] = [];
    for (const row of rows) {
      const files = db
        .prepare(`SELECT * FROM job_files WHERE jobId = ? ORDER BY fileIndex ASC`)
        .all(row.jobId) as Array<{
        jobId: string;
        fileIndex: number;
        originalname: string;
        path: string;
        size: number | null;
      }>;
      const tasks = db
        .prepare(`SELECT * FROM job_tasks WHERE jobId = ? ORDER BY fileIndex ASC, provider ASC`)
        .all(row.jobId) as Array<{
        jobId: string;
        fileIndex: number;
        provider: ProviderId;
        status: PersistedTaskStatus;
        attempts: number;
        lastError: string | null;
      }>;
      jobs.push(this.composeJob(row, files, tasks));
    }
    return jobs;
  }

  async deleteJob(jobId: string): Promise<void> {
    const db = this.requireDb();
    db.prepare(`DELETE FROM jobs WHERE jobId = ?`).run(jobId);
  }

  private composeJob(
    row: PersistedBatchJobRow,
    files: Array<{ fileIndex: number; originalname: string; path: string; size: number | null }>,
    tasks: Array<{
      fileIndex: number;
      provider: ProviderId;
      status: PersistedTaskStatus;
      attempts: number;
      lastError: string | null;
    }>
  ): PersistedBatchJob {
    return {
      jobId: row.jobId,
      providers: parseJsonOr<ProviderId[]>(row.providersJson, []),
      lang: row.lang,
      total: row.total,
      done: row.done,
      failed: row.failed,
      status: row.status,
      errors: parseJsonOr<{ file: string; message: string }[]>(row.errorsJson, []),
      warnings: parseJsonOr<JobWarning[]>(row.warningsJson, []),
      manifest: parseJsonOr<EvaluationManifest | undefined>(row.manifestJson, undefined),
      options: parseJsonOr<TranscriptionOptions | undefined>(row.optionsJson, undefined),
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      files: files.map((file) => ({
        index: file.fileIndex,
        originalname: file.originalname,
        path: file.path,
        size: file.size ?? undefined,
      })),
      tasks: tasks.map((task) => ({
        fileIndex: task.fileIndex,
        provider: task.provider,
        status: task.status,
        attempts: task.attempts,
        lastError: task.lastError ?? undefined,
      })),
    };
  }

  private composeJobSummary(
    row: Pick<
      PersistedBatchJobRow,
      'jobId' | 'providersJson' | 'lang' | 'total' | 'done' | 'failed' | 'status' | 'errorsJson' | 'warningsJson' | 'createdAt' | 'updatedAt'
    >
  ): PersistedBatchJobSummary {
    return {
      jobId: row.jobId,
      providers: parseJsonOr<ProviderId[]>(row.providersJson, []),
      lang: row.lang,
      total: row.total,
      done: row.done,
      failed: row.failed,
      status: row.status,
      errors: parseJsonOr<{ file: string; message: string }[]>(row.errorsJson, []),
      warnings: parseJsonOr<JobWarning[]>(row.warningsJson, []),
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    };
  }

  private requireDb(): Database.Database {
    if (!this.db) {
      throw new Error('PersistentBatchJobStore not initialized');
    }
    return this.db;
  }
}
