import { randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { copyFile, mkdir, rename, unlink } from 'node:fs/promises';
import { once } from 'node:events';
import path from 'node:path';
import os from 'node:os';
import { Readable } from 'node:stream';
import { buffer as streamToBuffer } from 'node:stream/consumers';
import type {
  BatchResult,
  BatchJobFileResult,
  EvaluationManifest,
  JobWarning,
  NormalizationConfig,
  ProviderAdapter,
  ProviderId,
  StorageDriver,
  StreamingOptions,
  TranscriptionOptions,
} from '../types.js';
import { loadConfig } from '../config.js';
import { logger } from '../logger.js';
import { getAdapter } from '../adapters/index.js';
import { convertToPcmReadable } from '../utils/ffmpeg.js';
import { ensureNormalizedAudio, AudioValidationError } from '../utils/audioIngress.js';
import { AudioDecodeError } from '../utils/audioNormalizer.js';
import { cer, rtf, wer } from '../scoring/metrics.js';
import { matchManifestItem, ManifestMatchError } from '../utils/manifest.js';
import { summarizeJob, summarizeJobByProvider } from '../utils/summary.js';
import type { JobSummary } from '../utils/summary.js';
import type { JobHistory } from './jobHistory.js';
import type { JobExportService } from './jobExportService.js';
import type {
  PersistedBatchJob,
  PersistedTaskStatus,
} from './persistentBatchJobStore.js';
import type { PersistentBatchJobStore } from './persistentBatchJobStore.js';

interface EnqueueFileInput {
  originalname: string;
  path: string;
  size?: number;
}

interface FileInput extends EnqueueFileInput {
  index: number;
}

interface PlannedFileInput extends FileInput {
  sourcePath: string;
}

type TaskState = PersistedTaskStatus;

interface JobState {
  id: string;
  providers: ProviderId[];
  providerOrderOffset: number;
  lang: string;
  total: number;
  files: FileInput[];
  done: number;
  failed: number;
  status: 'queued' | 'running' | 'completed' | 'failed';
  taskStates: Map<string, TaskState>;
  results: BatchJobFileResult[];
  manifest?: EvaluationManifest;
  options?: TranscriptionOptions;
  normalization?: NormalizationConfig;
  errors: { file: string; message: string }[];
  warnings: JobWarning[];
  degradedCount: number;
}

interface PreparedFile {
  normalization: Awaited<ReturnType<typeof ensureNormalizedAudio>>;
  refText?: string;
  durationSec: number;
  degraded: boolean;
  pcmBuffer: Buffer;
}

interface Semaphore {
  acquire: () => Promise<() => void>;
  setLimit: (limit: number) => void;
  getLimit: () => number;
}

interface ProviderRuntimeReporter {
  recordProviderSuccess(provider: ProviderId): void;
  recordProviderFailure(provider: ProviderId, reason?: string): void;
}

interface BatchRetryPolicy {
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

export interface BatchJobListEntry {
  jobId: string;
  provider: string;
  providers: string[];
  lang: string;
  createdAt: string;
  updatedAt: string;
  total: number;
  done: number;
  failed: number;
  status: 'queued' | 'running' | 'completed' | 'failed';
  errors: { file: string; message: string }[];
  warnings: JobWarning[];
  resultCount: number;
  summary: JobSummary;
  summaryByProvider?: Record<string, JobSummary>;
}

function createSemaphore(limit: number): Semaphore {
  let effectiveLimit = Math.max(1, Math.floor(limit));
  let active = 0;
  const waiters: (() => void)[] = [];

  const drainWaiters = () => {
    while (active < effectiveLimit && waiters.length > 0) {
      const next = waiters.shift();
      if (!next) break;
      active += 1;
      next();
    }
  };

  const releaseActiveSlot = () => {
    active = Math.max(0, active - 1);
    drainWaiters();
  };

  return {
    async acquire() {
      if (active >= effectiveLimit) {
        await new Promise<void>((resolve) => {
          waiters.push(resolve);
        });
      } else {
        active += 1;
      }

      let released = false;
      return () => {
        if (released) return;
        released = true;
        releaseActiveSlot();
      };
    },
    setLimit(nextLimit: number) {
      effectiveLimit = Math.max(1, Math.floor(nextLimit));
      drainWaiters();
    },
    getLimit() {
      return effectiveLimit;
    },
  };
}

export class BatchRunner {
  private jobs = new Map<string, JobState>();
  private cleanupTimers = new Map<string, NodeJS.Timeout>();
  private jobStore?: PersistentBatchJobStore;
  private readonly globalTaskSemaphore = createSemaphore(1);
  private readonly providerSemaphores = new Map<ProviderId, Semaphore>();

  constructor(
    private storage: StorageDriver<BatchJobFileResult>,
    private readonly jobHistory: JobHistory,
    private readonly jobExporter?: JobExportService,
    jobStore?: PersistentBatchJobStore,
    private readonly providerRuntimeReporter?: ProviderRuntimeReporter
  ) {
    this.jobStore = jobStore;
  }

  async init(): Promise<void> {
    await this.storage.init();
    if (!this.jobStore) return;
    try {
      await this.jobStore.init();
      await this.restoreActiveJobs();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'failed to initialize persistent job store';
      throw new Error(`persistent batch job store initialization failed: ${message}`);
    }
  }

  async enqueue(
    providers: ProviderId[],
    lang: string,
    files: EnqueueFileInput[],
    manifest?: EvaluationManifest,
    options?: TranscriptionOptions
  ): Promise<{ jobId: string; queued: number }> {
    if (files.length === 0) {
      throw new Error('No files uploaded');
    }

    const config = await loadConfig();
    const uniqProviders = Array.from(new Set(providers));
    const jobId = randomUUID();
    const plannedFiles = this.planInputFiles(jobId, files, config.storage.path);

    let stagedFiles: FileInput[] = [];
    let persistedCreated = false;
    try {
      if (this.jobStore) {
        await this.jobStore.createJob({
          jobId,
          providers: uniqProviders,
          lang,
          manifest,
          options,
          files: plannedFiles.map((file) => ({
            index: file.index,
            originalname: file.originalname,
            path: file.path,
            size: file.size,
          })),
        });
        persistedCreated = true;
      }

      stagedFiles = await this.stageInputFiles(plannedFiles);
      const taskStates = this.createInitialTaskStates(stagedFiles, uniqProviders);
      const job: JobState = {
        id: jobId,
        providers: uniqProviders,
        providerOrderOffset: this.computeProviderOrderOffset(jobId, uniqProviders.length),
        lang,
        total: stagedFiles.length * uniqProviders.length,
        files: stagedFiles,
        done: 0,
        failed: 0,
        status: 'queued',
        taskStates,
        results: [],
        manifest,
        options,
        normalization: manifest?.normalization ?? config.normalization,
        errors: [],
        warnings: [],
        degradedCount: 0,
      };

      this.jobs.set(jobId, job);
      void this.processJob(job);
      return { jobId, queued: stagedFiles.length };
    } catch (error) {
      await Promise.all(stagedFiles.map((file) => unlink(file.path).catch(() => undefined)));
      await Promise.all(plannedFiles.map((file) => unlink(file.path).catch(() => undefined)));
      await Promise.all(files.map((file) => unlink(file.path).catch(() => undefined)));
      if (persistedCreated && this.jobStore) {
        await this.jobStore.deleteJob(jobId).catch(() => undefined);
      }
      throw error;
    }
  }

  getStatus(jobId: string) {
    const job = this.jobs.get(jobId);
    if (!job) return null;
    return {
      jobId,
      total: job.total,
      done: job.done,
      failed: job.failed,
      errors: job.errors,
      warnings: job.warnings,
      providers: job.providers,
      status: job.status,
    };
  }

  async getStatusIncludingPersisted(jobId: string) {
    const live = this.getStatus(jobId);
    if (live) return live;
    if (!this.jobStore) return null;
    return this.jobStore.getJobStatus(jobId);
  }

  async getResults(jobId: string): Promise<BatchJobFileResult[] | null> {
    const job = this.jobs.get(jobId);
    if (job) return job.results;
    if (typeof this.storage.readByJob === 'function') {
      const stored = await this.storage.readByJob(jobId);
      if (stored.length > 0) return stored;
    }
    if (this.jobStore) {
      const status = await this.jobStore.getJobStatus(jobId);
      if (status) {
        return [];
      }
    }
    return null;
  }

  async listJobs(limit = 200): Promise<BatchJobListEntry[]> {
    const safeLimit = Number.isFinite(limit) ? Math.max(1, Math.min(1000, Math.floor(limit))) : 200;
    if (!this.jobStore) {
      const legacy = await this.jobHistory.list();
      return legacy.slice(0, safeLimit).map((entry) => ({
        ...entry,
        done: entry.total,
        failed: 0,
        status: 'completed',
        errors: [],
        warnings: [],
        resultCount: entry.total,
      }));
    }

    const persisted = await this.jobStore.listJobs(safeLimit);
    const entries: BatchJobListEntry[] = [];
    for (const row of persisted) {
      const resultRows = typeof this.storage.readByJob === 'function' ? await this.storage.readByJob(row.jobId) : [];
      entries.push({
        jobId: row.jobId,
        provider: row.providers[0] ?? 'unknown',
        providers: row.providers,
        lang: row.lang,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
        total: row.total,
        done: row.done,
        failed: row.failed,
        status: row.status,
        errors: row.errors,
        warnings: row.warnings,
        resultCount: resultRows.length,
        summary: summarizeJob(resultRows),
        summaryByProvider: summarizeJobByProvider(resultRows),
      });
    }
    return entries;
  }

  private async restoreActiveJobs(): Promise<void> {
    if (!this.jobStore) return;
    const active = await this.jobStore.listActiveJobs();
    for (const persisted of active) {
      await this.jobStore.resetRunningTasks(persisted.jobId);
      const refreshed = await this.jobStore.getJob(persisted.jobId);
      if (!refreshed) continue;
      const job = await this.toJobStateFromPersisted(refreshed);
      this.jobs.set(job.id, job);
      void this.processJob(job);
    }
  }

  private async toJobStateFromPersisted(persisted: PersistedBatchJob): Promise<JobState> {
    const taskStates = new Map<string, TaskState>();
    for (const task of persisted.tasks) {
      const state: TaskState = task.status === 'running' ? 'pending' : task.status;
      taskStates.set(this.taskKey(task.fileIndex, task.provider), state);
    }

    const results = typeof this.storage.readByJob === 'function' ? await this.storage.readByJob(persisted.jobId) : [];
    const done = this.countTaskStates(taskStates, 'done');
    const failed = this.countTaskStates(taskStates, 'failed');

    return {
      id: persisted.jobId,
      providers: persisted.providers,
      providerOrderOffset: this.computeProviderOrderOffset(persisted.jobId, persisted.providers.length),
      lang: persisted.lang,
      total: persisted.total,
      files: persisted.files.map((file) => ({
        index: file.index,
        originalname: file.originalname,
        path: file.path,
        size: file.size,
      })),
      done,
      failed,
      status: persisted.status === 'queued' ? 'queued' : 'running',
      taskStates,
      results,
      manifest: persisted.manifest,
      options: persisted.options,
      normalization: persisted.manifest?.normalization,
      errors: [...persisted.errors],
      warnings: [...persisted.warnings],
      degradedCount: results.reduce((count, row) => (row.degraded ? count + 1 : count), 0),
    };
  }

  private async processJob(job: JobState): Promise<void> {
    let config: Awaited<ReturnType<typeof loadConfig>> | null = null;
    let fatalJobError = false;
    try {
      const loadedConfig = await loadConfig();
      config = loadedConfig;
      job.status = 'running';
      if (this.jobStore) {
        await this.jobStore.setJobStatus(job.id, 'running');
      }

      const cpuCount = Math.max(1, os.cpus().length || 1);
      const configuredMaxParallel = loadedConfig.jobs?.maxParallel ?? 4;
      const maxParallel = Math.max(1, Math.min(cpuCount, configuredMaxParallel));
      if (this.globalTaskSemaphore.getLimit() !== maxParallel) {
        this.globalTaskSemaphore.setLimit(maxParallel);
      }
      const retryPolicy = this.resolveRetryPolicy(loadedConfig);
      const providerCount = job.providers.length;
      const requestedParallel = Math.max(1, job.options?.parallel ?? 1);
      const desiredSlots = providerCount * requestedParallel;
      const effectiveSlots = Math.min(maxParallel, Math.max(providerCount, desiredSlots));
      const fileConcurrency = Math.max(1, Math.floor(effectiveSlots / providerCount) || 1);
      const providerConcurrency = Math.max(
        1,
        Math.min(providerCount, Math.floor(maxParallel / fileConcurrency) || providerCount)
      );
      if (providerConcurrency < providerCount || fileConcurrency * providerConcurrency < desiredSlots) {
        await this.addWarning(job, {
          code: 'parallel_clamped',
          message:
            'Requested parallelism was clamped by server limits. Increase jobs.maxParallel or lower options.parallel for fairer concurrency.',
          details: {
            providers: providerCount,
            requestedParallelFiles: requestedParallel,
            effectiveSlots,
            maxParallel,
            fileConcurrency,
            providerConcurrency,
          },
        });
        logger.warn({
          event: 'batch_parallel_clamped',
          providers: providerCount,
          requestedParallelFiles: requestedParallel,
          effectiveSlots,
          maxParallel,
          fileConcurrency,
          providerConcurrency,
        });
      }

      const providerCaps = new Map<ProviderId, number>();
      for (const provider of job.providers) {
        const providerCap = loadedConfig.jobs?.providerMaxParallel?.[provider] ?? maxParallel;
        providerCaps.set(provider, providerCap);
        this.resolveProviderSemaphore(provider, maxParallel, providerCap);
      }

      let fileCursor = 0;
      const runProvidersWithLimit = async (
        providers: ProviderId[],
        limit: number,
        fn: (provider: ProviderId) => Promise<void>
      ) => {
        let idx = 0;
        const providerWorker = async () => {
          while (idx < providers.length) {
            const current = providers[idx];
            idx += 1;
            await fn(current);
          }
        };
        await Promise.all(Array.from({ length: limit }, () => providerWorker()));
      };

      const fileWorker = async () => {
        while (fileCursor < job.files.length) {
          const index = fileCursor;
          fileCursor += 1;
          const file = job.files[index];
          if (!file) break;

          const pendingProviders = this.pendingProvidersForFile(job, file.index);
          if (pendingProviders.length === 0) {
            await unlink(file.path).catch(() => undefined);
            continue;
          }

          let prepared: PreparedFile | null = null;
          try {
            prepared = await this.prepareFile(file, job, loadedConfig);
          } catch (error) {
            const message =
              error instanceof ManifestMatchError
                ? error.message
                : error instanceof AudioValidationError || error instanceof AudioDecodeError
                  ? 'audio decode failed (unsupported or corrupted file)'
                  : error instanceof Error
                    ? error.message
                    : 'Unknown adapter error';
            for (const provider of pendingProviders) {
              await this.markTaskFailed(job, file, provider, message);
            }
            await this.cleanupPreparedFile(file, prepared);
            continue;
          }

          await runProvidersWithLimit(pendingProviders, providerConcurrency, async (provider) => {
            const providerCap = providerCaps.get(provider) ?? maxParallel;
            const semaphore = this.resolveProviderSemaphore(provider, maxParallel, providerCap);
            const releaseGlobalSlot = await this.globalTaskSemaphore.acquire();
            let releaseProviderSlot: (() => void) | null = null;
            try {
              releaseProviderSlot = await semaphore.acquire();
              if (!this.transitionTaskState(job, file.index, provider, 'running')) {
                return;
              }
              if (this.jobStore) {
                await this.jobStore.markTaskRunning(job.id, file.index, provider);
              }
              await this.runProviderTask(file, provider, prepared as PreparedFile, job, loadedConfig, retryPolicy);
              if (this.transitionTaskState(job, file.index, provider, 'done') && this.jobStore) {
                await this.jobStore.markTaskDone(job.id, file.index, provider);
              }
            } catch (error) {
              const message =
                error instanceof ManifestMatchError
                  ? error.message
                  : error instanceof AudioValidationError || error instanceof AudioDecodeError
                    ? 'audio decode failed (unsupported or corrupted file)'
                    : error instanceof Error
                      ? error.message
                      : 'Unknown adapter error';
              await this.markTaskFailed(job, file, provider, message);
            } finally {
              releaseProviderSlot?.();
              releaseGlobalSlot();
            }
          });

          await this.cleanupPreparedFile(file, prepared);
        }
      };

      const workers = Array.from({ length: fileConcurrency }, () => fileWorker());
      const settled = await Promise.allSettled(workers);
      const rejected = settled.filter((result) => result.status === 'rejected') as PromiseRejectedResult[];
      if (rejected.length > 0) {
        throw rejected[0].reason instanceof Error ? rejected[0].reason : new Error('batch worker failed');
      }
    } catch (error) {
      fatalJobError = true;
      const message = error instanceof Error ? error.message : 'Unknown batch runner error';
      logger.error({ event: 'batch_job_failed', jobId: job.id, message });
      job.errors.push({ file: '(job)', message });
      await this.failRemainingTasks(job, message);
      await Promise.all(job.files.map((file) => unlink(file.path).catch(() => undefined)));
    } finally {
      if (job.results.length > 0 && job.degradedCount > 0) {
        const degradedRatio = job.degradedCount / job.results.length;
        await this.addWarning(job, {
          code: 'degraded_audio_detected',
          message:
            'Some files required degraded normalization fallback. Review degraded count/ratio in job summary before comparing providers.',
          details: {
            degradedCount: job.degradedCount,
            resultCount: job.results.length,
            degradedRatio,
          },
        });
      }

      if (this.jobExporter && job.results.length > 0) {
        try {
          await this.jobExporter.export(job.id, job.results);
        } catch {
          // errors already logged by JobExportService
        }
      }

      job.status = fatalJobError ? 'failed' : 'completed';
      if (this.jobStore) {
        await this.jobStore.setJobStatus(job.id, fatalJobError ? 'failed' : 'completed');
      }

      job.files = [];
      job.manifest = undefined;
      job.options = undefined;
      const retentionMs = config?.jobs?.retentionMs ?? 10 * 60 * 1000;
      const timer = setTimeout(() => {
        this.jobs.delete(job.id);
        this.cleanupTimers.delete(job.id);
      }, retentionMs);
      this.cleanupTimers.set(job.id, timer);
    }
  }

  private async prepareFile(
    file: FileInput,
    job: JobState,
    config: Awaited<ReturnType<typeof loadConfig>>
  ): Promise<PreparedFile> {
    const manifestItem = job.manifest ? matchManifestItem(job.manifest, file.originalname) : undefined;
    if (job.manifest && !manifestItem) {
      throw new ManifestMatchError('manifest ref not found for file');
    }

    const normalization = await ensureNormalizedAudio(file.path, { config, allowCache: true });
    let sourceStream: ReturnType<typeof createReadStream> | null = null;
    try {
      sourceStream = createReadStream(normalization.normalizedPath);
      await once(sourceStream, 'open');
      const { stream: pcmStream, durationPromise } = await convertToPcmReadable(sourceStream);
      const pcmBuffer = await streamToBuffer(pcmStream);
      const durationSec = normalization.durationSec ?? (await durationPromise);
      if (!durationSec || !Number.isFinite(durationSec)) {
        throw new Error('duration could not be determined');
      }

      return {
        normalization,
        refText: manifestItem?.ref,
        durationSec,
        degraded: normalization.degraded,
        pcmBuffer,
      };
    } catch (error) {
      sourceStream?.destroy();
      await normalization.release().catch(() => undefined);
      throw error;
    }
  }

  private async runProviderTask(
    file: FileInput,
    provider: ProviderId,
    prepared: PreparedFile,
    job: JobState,
    config: Awaited<ReturnType<typeof loadConfig>>,
    retryPolicy: BatchRetryPolicy
  ): Promise<void> {
    const adapter = getAdapter(provider);
    const sampleRateHz = config.audio.targetSampleRate;
    const evaluationLang = job.manifest?.language?.trim() || job.lang;
    const streamingOpts: StreamingOptions = {
      language: evaluationLang,
      sampleRateHz,
      encoding: 'linear16',
      enableInterim: false,
      contextPhrases: job.options?.dictionaryPhrases,
      punctuationPolicy: job.options?.punctuationPolicy,
      enableVad: job.options?.enableVad ?? false,
      dictionaryPhrases: job.options?.dictionaryPhrases,
    };

    const start = Date.now();
    let batchResult: BatchResult;
    try {
      batchResult = await this.transcribeWithRetry(
        adapter,
        prepared.pcmBuffer,
        streamingOpts,
        provider,
        file.originalname,
        retryPolicy
      );
      this.providerRuntimeReporter?.recordProviderSuccess(provider);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown adapter error';
      this.providerRuntimeReporter?.recordProviderFailure(provider, message);
      throw error;
    }
    const processingTimeMs = Date.now() - start;
    const computedDuration =
      prepared.pcmBuffer.length / (2 * config.audio.targetChannels * sampleRateHz);
    const adapterDurationSec =
      typeof batchResult.durationSec === 'number' &&
      Number.isFinite(batchResult.durationSec) &&
      batchResult.durationSec > 0
        ? batchResult.durationSec
        : undefined;
    const durationSec = adapterDurationSec ?? prepared.durationSec ?? computedDuration;

    const normalization = job.normalization ?? config.normalization;
    const isJapanese = evaluationLang.toLowerCase().startsWith('ja');

    const score: BatchJobFileResult = {
      jobId: job.id,
      path: file.originalname,
      provider,
      lang: evaluationLang,
      durationSec,
      processingTimeMs,
      rtf: durationSec ? rtf(processingTimeMs, durationSec) : 0,
      cer: prepared.refText ? cer(prepared.refText, batchResult.text, normalization) : undefined,
      wer: prepared.refText && !isJapanese ? wer(prepared.refText, batchResult.text, normalization) : undefined,
      latencyMs: processingTimeMs,
      text: batchResult.text,
      refText: prepared.refText,
      degraded: prepared.degraded,
      createdAt: new Date().toISOString(),
      opts: job.options as Record<string, unknown>,
      vendorProcessingMs: batchResult.vendorProcessingMs,
      normalizationUsed: normalization,
      audioSpec: {
        sampleRateHz,
        channels: config.audio.targetChannels,
        encoding: 'linear16',
      },
    };
    await this.storage.append(score);
    job.results.push(score);
    if (score.degraded) {
      job.degradedCount += 1;
    }
    this.jobHistory.recordRow(score);
  }

  private resolveRetryPolicy(config: Awaited<ReturnType<typeof loadConfig>>): BatchRetryPolicy {
    const retry = config.jobs?.retry;
    return {
      maxAttempts: retry?.maxAttempts ?? 3,
      baseDelayMs: retry?.baseDelayMs ?? 1000,
      maxDelayMs: retry?.maxDelayMs ?? 10_000,
    };
  }

  private isRetryableBatchError(error: unknown): boolean {
    if (!(error instanceof Error)) return false;
    const text = error.message.toLowerCase();
    if (/\b(408|409|425|429|500|502|503|504)\b/.test(text)) {
      return true;
    }
    return (
      text.includes('timeout') ||
      text.includes('timed out') ||
      text.includes('failed to fetch') ||
      text.includes('network') ||
      text.includes('econnreset') ||
      text.includes('socket hang up') ||
      text.includes('etimedout') ||
      text.includes('eai_again')
    );
  }

  private nextRetryDelayMs(attempt: number, retryPolicy: BatchRetryPolicy): number {
    const jitter = Math.floor(Math.random() * 250);
    const exponential = retryPolicy.baseDelayMs * 2 ** (attempt - 1);
    return Math.min(exponential + jitter, retryPolicy.maxDelayMs);
  }

  private async transcribeWithRetry(
    adapter: ProviderAdapter,
    pcmBuffer: Buffer,
    streamingOpts: StreamingOptions,
    provider: ProviderId,
    filename: string,
    retryPolicy: BatchRetryPolicy
  ): Promise<BatchResult> {
    let lastError: Error | null = null;
    for (let attempt = 1; attempt <= retryPolicy.maxAttempts; attempt += 1) {
      try {
        if (typeof adapter.transcribePcmBuffer === 'function') {
          return await adapter.transcribePcmBuffer(pcmBuffer, streamingOpts);
        }
        return await adapter.transcribeFileFromPCM(Readable.from(pcmBuffer), streamingOpts);
      } catch (error) {
        const normalizedError =
          error instanceof Error ? error : new Error(typeof error === 'string' ? error : 'Unknown adapter error');
        lastError = normalizedError;
        if (!this.isRetryableBatchError(normalizedError) || attempt >= retryPolicy.maxAttempts) {
          throw normalizedError;
        }
        const delayMs = this.nextRetryDelayMs(attempt, retryPolicy);
        logger.warn({
          event: 'batch_retry_scheduled',
          provider,
          file: filename,
          attempt,
          delayMs,
          message: normalizedError.message,
        });
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
    throw lastError ?? new Error('Unknown adapter error');
  }

  private async markTaskFailed(job: JobState, file: FileInput, provider: ProviderId, message: string): Promise<void> {
    if (!this.transitionTaskState(job, file.index, provider, 'failed')) {
      return;
    }
    job.errors.push({ file: `${file.originalname} (${provider})`, message });
    logger.error({
      event: 'batch_failed',
      file: file.originalname,
      provider,
      message,
    });
    if (this.jobStore) {
      await this.jobStore.markTaskFailed(job.id, file.index, provider, {
        file: `${file.originalname} (${provider})`,
        message,
      });
    }
  }

  private async failRemainingTasks(job: JobState, message: string): Promise<void> {
    for (const file of job.files) {
      for (const provider of job.providers) {
        const state = this.getTaskState(job, file.index, provider);
        if (state !== 'pending' && state !== 'running') {
          continue;
        }
        await this.markTaskFailed(job, file, provider, message);
      }
    }
  }

  private taskKey(fileIndex: number, provider: ProviderId): string {
    return `${fileIndex}::${provider}`;
  }

  private getTaskState(job: JobState, fileIndex: number, provider: ProviderId): TaskState {
    return job.taskStates.get(this.taskKey(fileIndex, provider)) ?? 'pending';
  }

  private transitionTaskState(
    job: JobState,
    fileIndex: number,
    provider: ProviderId,
    next: TaskState
  ): boolean {
    const key = this.taskKey(fileIndex, provider);
    const prev = job.taskStates.get(key) ?? 'pending';
    if (prev === 'done' || prev === 'failed') {
      return false;
    }
    if (next === 'running' && prev !== 'pending') {
      return false;
    }
    job.taskStates.set(key, next);
    if (next === 'done') {
      job.done += 1;
    }
    if (next === 'failed') {
      job.failed += 1;
    }
    return true;
  }

  private pendingProvidersForFile(job: JobState, fileIndex: number): ProviderId[] {
    const providerCount = job.providers.length;
    if (providerCount === 0) return [];
    const start = (job.providerOrderOffset + fileIndex) % providerCount;
    const orderedProviders =
      start === 0
        ? job.providers
        : [...job.providers.slice(start), ...job.providers.slice(0, start)];
    return orderedProviders.filter((provider) => this.getTaskState(job, fileIndex, provider) === 'pending');
  }

  private computeProviderOrderOffset(jobId: string, providerCount: number): number {
    if (providerCount <= 1) return 0;
    let hash = 0;
    for (let index = 0; index < jobId.length; index += 1) {
      hash = (hash * 31 + jobId.charCodeAt(index)) >>> 0;
    }
    return hash % providerCount;
  }

  private resolveProviderSemaphore(provider: ProviderId, maxParallel: number, providerCap: number): Semaphore {
    const effectiveLimit = Math.max(1, Math.min(maxParallel, providerCap));
    const existing = this.providerSemaphores.get(provider);
    if (existing) {
      existing.setLimit(effectiveLimit);
      return existing;
    }
    const created = createSemaphore(effectiveLimit);
    this.providerSemaphores.set(provider, created);
    return created;
  }

  private countTaskStates(taskStates: Map<string, TaskState>, state: TaskState): number {
    let count = 0;
    for (const value of taskStates.values()) {
      if (value === state) count += 1;
    }
    return count;
  }

  private createInitialTaskStates(files: FileInput[], providers: ProviderId[]): Map<string, TaskState> {
    const states = new Map<string, TaskState>();
    for (const file of files) {
      for (const provider of providers) {
        states.set(this.taskKey(file.index, provider), 'pending');
      }
    }
    return states;
  }

  private async stageInputFiles(
    plannedFiles: PlannedFileInput[]
  ): Promise<FileInput[]> {
    if (plannedFiles.length === 0) return [];
    const inputsDir = path.dirname(plannedFiles[0].path);
    await mkdir(inputsDir, { recursive: true });

    const staged: FileInput[] = [];
    for (const file of plannedFiles) {
      await this.moveFileToPath(file.sourcePath, file.path);
      staged.push({
        index: file.index,
        originalname: file.originalname,
        path: file.path,
        size: file.size,
      });
    }
    return staged;
  }

  private planInputFiles(
    jobId: string,
    files: EnqueueFileInput[],
    storagePath: string
  ): PlannedFileInput[] {
    const inputsDir = path.resolve(storagePath, 'jobs', jobId, 'inputs');
    const planned: PlannedFileInput[] = [];
    for (let index = 0; index < files.length; index += 1) {
      const file = files[index];
      if (!file) continue;
      const stagedName = this.buildStagedName(index, file.originalname);
      const stagedPath = path.resolve(inputsDir, stagedName);
      planned.push({
        index,
        originalname: file.originalname,
        path: stagedPath,
        size: file.size,
        sourcePath: file.path,
      });
    }
    return planned;
  }

  private buildStagedName(index: number, originalname: string): string {
    const extRaw = path.extname(originalname);
    const ext = extRaw.replace(/[^a-zA-Z0-9.]/g, '').slice(0, 10);
    const baseRaw = path.basename(originalname, extRaw);
    const base = baseRaw.replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 80) || 'audio';
    return `${String(index).padStart(4, '0')}-${randomUUID()}-${base}${ext}`;
  }

  private async moveFileToPath(sourcePath: string, targetPath: string): Promise<void> {
    try {
      await rename(sourcePath, targetPath);
      return;
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code !== 'EXDEV') {
        throw error;
      }
    }
    await copyFile(sourcePath, targetPath);
    await unlink(sourcePath).catch(() => undefined);
  }

  private async cleanupPreparedFile(file: FileInput, prepared: PreparedFile | null): Promise<void> {
    if (prepared?.normalization) {
      await prepared.normalization.release().catch(() => undefined);
    }
    await unlink(file.path).catch(() => undefined);
  }

  private async addWarning(job: JobState, warning: Omit<JobWarning, 'createdAt'>): Promise<void> {
    if (job.warnings.some((entry) => entry.code === warning.code)) {
      return;
    }
    const enriched: JobWarning = {
      ...warning,
      createdAt: new Date().toISOString(),
    };
    job.warnings.push(enriched);
    if (this.jobStore) {
      await this.jobStore.addJobWarning(job.id, enriched);
    }
  }
}
