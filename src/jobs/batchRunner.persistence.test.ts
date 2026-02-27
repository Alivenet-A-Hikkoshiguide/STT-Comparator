import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { PassThrough } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Database from 'better-sqlite3';
import type { BatchJobFileResult, StorageDriver } from '../types.js';

const tempDirs: string[] = [];
let sqliteAvailable = true;
try {
  const probe = new Database(':memory:');
  probe.close();
} catch {
  sqliteAvailable = false;
}
const maybeIt = sqliteAvailable ? it : it.skip;

const memoryStore = (): StorageDriver<BatchJobFileResult> & { records: BatchJobFileResult[] } => {
  const records: BatchJobFileResult[] = [];
  return {
    records,
    init: async () => {},
    append: async (row: BatchJobFileResult) => {
      records.push(row);
    },
    readAll: async () => records,
  };
};

interface BatchJobStatus {
  jobId: string;
  total: number;
  done: number;
  failed: number;
  status: 'queued' | 'running' | 'completed' | 'failed';
}

const waitForJobCompletion = async (
  runner: { getStatusIncludingPersisted: (id: string) => Promise<BatchJobStatus | null> },
  jobId: string,
  timeoutMs = 2500
) => {
  const start = Date.now();
  let last: BatchJobStatus | null = null;
  while (Date.now() - start < timeoutMs) {
    const status = await runner.getStatusIncludingPersisted(jobId);
    last = status;
    if (status && status.done + status.failed >= status.total && status.status === 'completed') {
      return status;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`persisted job did not complete in time: ${JSON.stringify(last)}`);
};

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
  vi.clearAllMocks();
});

beforeEach(() => {
  vi.resetModules();
});

describe('BatchRunner persistence', () => {
  maybeIt('restores active jobs and resumes pending tasks after restart', async () => {
    const tempDir = await mkdtemp(path.join(tmpdir(), 'batch-runner-persist-'));
    tempDirs.push(tempDir);
    const storagePath = path.join(tempDir, 'runs');
    const audioPath = path.join(tempDir, 'sample.wav');
    await writeFile(audioPath, Buffer.from('audio'));

    vi.doMock('../config.js', () => ({
      loadConfig: vi.fn().mockResolvedValue({
        audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
        ingressNormalize: { enabled: true },
        normalization: {},
        storage: { driver: 'jsonl', path: storagePath },
        providers: ['mock'],
        jobs: {
          maxParallel: 2,
          retentionMs: 10 * 60 * 1000,
          retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
        },
      }),
    }));

    const release = vi.fn(async () => {});
    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (filePath: string) => ({
        normalizedPath: filePath,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release,
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end(Buffer.from([0, 1, 2, 3]));
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(1),
        })),
      };
    });

    const transcribeFileFromPCM = vi.fn(async () => ({ provider: 'mock', text: 'resumed', durationSec: 1 }));
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM,
      })),
    }));

    const [{ BatchRunner }, { JobHistory }, { PersistentBatchJobStore }] = await Promise.all([
      import('./batchRunner.js'),
      import('./jobHistory.js'),
      import('./persistentBatchJobStore.js'),
    ]);

    const store = memoryStore();
    const history = new JobHistory(store);
    await history.init();

    const jobId = 'job-restore';
    const jobStore = new PersistentBatchJobStore(path.join(tempDir, 'batch-jobs.sqlite'));
    await jobStore.init();
    await jobStore.createJob({
      jobId,
      providers: ['mock'],
      lang: 'ja-JP',
      files: [{ index: 0, originalname: 'sample.wav', path: audioPath, size: 1 }],
    });
    await jobStore.setJobStatus(jobId, 'running');
    await jobStore.markTaskRunning(jobId, 0, 'mock');

    const runner = new BatchRunner(store, history, undefined, jobStore);
    await runner.init();

    const status = await waitForJobCompletion(runner, jobId);
    expect(status).toMatchObject({
      jobId,
      total: 1,
      done: 1,
      failed: 0,
      status: 'completed',
    });
    expect(store.records).toHaveLength(1);
    expect(store.records[0]?.text).toBe('resumed');
    expect(transcribeFileFromPCM).toHaveBeenCalledTimes(1);
    expect(release).toHaveBeenCalledTimes(1);

    const persisted = await jobStore.getJob(jobId);
    expect(persisted?.status).toBe('completed');
    expect(persisted?.tasks[0]).toMatchObject({ status: 'done', attempts: 2 });
  });

  maybeIt('lists failed-only jobs and resolves empty results for persisted jobs', async () => {
    const tempDir = await mkdtemp(path.join(tmpdir(), 'batch-runner-persist-'));
    tempDirs.push(tempDir);

    vi.doMock('../config.js', () => ({
      loadConfig: vi.fn().mockResolvedValue({
        audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
        ingressNormalize: { enabled: true },
        normalization: {},
        storage: { driver: 'jsonl', path: path.join(tempDir, 'runs') },
        providers: ['mock'],
        jobs: {
          maxParallel: 2,
          retentionMs: 10 * 60 * 1000,
          retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
        },
      }),
    }));

    const [{ BatchRunner }, { JobHistory }, { PersistentBatchJobStore }] = await Promise.all([
      import('./batchRunner.js'),
      import('./jobHistory.js'),
      import('./persistentBatchJobStore.js'),
    ]);

    const store = memoryStore();
    const history = new JobHistory(store);
    await history.init();

    const jobStore = new PersistentBatchJobStore(path.join(tempDir, 'batch-jobs.sqlite'));
    await jobStore.init();
    await jobStore.createJob({
      jobId: 'job-failed',
      providers: ['mock'],
      lang: 'ja-JP',
      files: [{ index: 0, originalname: 'missing.wav', path: '/tmp/missing.wav', size: 1 }],
    });
    await jobStore.setJobStatus('job-failed', 'running');
    await jobStore.markTaskRunning('job-failed', 0, 'mock');
    await jobStore.markTaskFailed('job-failed', 0, 'mock', {
      file: 'missing.wav (mock)',
      message: 'audio decode failed',
    });
    await jobStore.setJobStatus('job-failed', 'failed');

    const runner = new BatchRunner(store, history, undefined, jobStore);
    await runner.init();

    const listed = await runner.listJobs(20);
    expect(listed[0]).toMatchObject({
      jobId: 'job-failed',
      total: 1,
      done: 0,
      failed: 1,
      status: 'failed',
      resultCount: 0,
    });
    expect(listed[0]?.summary.count).toBe(0);

    await expect(runner.getResults('job-failed')).resolves.toEqual([]);
  });

  maybeIt('removes persisted shell jobs when input staging fails', async () => {
    const tempDir = await mkdtemp(path.join(tmpdir(), 'batch-runner-persist-'));
    tempDirs.push(tempDir);
    const missingFile = path.join(tempDir, 'missing.wav');

    vi.doMock('../config.js', () => ({
      loadConfig: vi.fn().mockResolvedValue({
        audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
        ingressNormalize: { enabled: true },
        normalization: {},
        storage: { driver: 'jsonl', path: path.join(tempDir, 'runs') },
        providers: ['mock'],
        jobs: {
          maxParallel: 2,
          retentionMs: 10 * 60 * 1000,
          retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 1 },
        },
      }),
    }));

    const [{ BatchRunner }, { JobHistory }, { PersistentBatchJobStore }] = await Promise.all([
      import('./batchRunner.js'),
      import('./jobHistory.js'),
      import('./persistentBatchJobStore.js'),
    ]);

    const store = memoryStore();
    const history = new JobHistory(store);
    await history.init();

    const jobStore = new PersistentBatchJobStore(path.join(tempDir, 'batch-jobs.sqlite'));
    await jobStore.init();

    const runner = new BatchRunner(store, history, undefined, jobStore);
    await runner.init();

    await expect(
      runner.enqueue(['mock'], 'ja-JP', [{ path: missingFile, originalname: 'missing.wav', size: 1 }])
    ).rejects.toThrow();

    const listed = await jobStore.listJobs(10);
    expect(listed).toHaveLength(0);
  });

  maybeIt('processes 100 files x 2 providers with retries using jsonl + sqlite persistence', async () => {
    const tempDir = await mkdtemp(path.join(tmpdir(), 'batch-runner-scale-'));
    tempDirs.push(tempDir);
    const inputDir = path.join(tempDir, 'inputs');
    const storagePath = path.join(tempDir, 'runs');
    await mkdir(inputDir, { recursive: true });

    const fileInputs: Array<{ path: string; originalname: string; size: number }> = [];
    for (let index = 0; index < 100; index += 1) {
      const originalname = `f-${String(index).padStart(3, '0')}.wav`;
      const payload = Buffer.from(String(index), 'utf-8');
      const filePath = path.join(inputDir, originalname);
      await writeFile(filePath, payload);
      fileInputs.push({ path: filePath, originalname, size: payload.length });
    }

    vi.doMock('../config.js', () => ({
      loadConfig: vi.fn().mockResolvedValue({
        audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
        ingressNormalize: { enabled: true },
        normalization: {},
        storage: { driver: 'jsonl', path: storagePath },
        providers: ['mock', 'openai'],
        jobs: {
          maxParallel: 8,
          retentionMs: 10 * 60 * 1000,
          persistenceMode: 'required',
          providerMaxParallel: { mock: 4, openai: 3 },
          retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 5 },
        },
      }),
    }));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (filePath: string) => ({
        normalizedPath: filePath,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: `sig-${path.basename(filePath)}`,
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async (source: NodeJS.ReadableStream) => ({
        stream: source,
        durationPromise: Promise.resolve(1),
      })),
    }));

    const seenByProvider = new Map<string, Set<number>>();
    const openaiAttempts = new Map<number, number>();
    const registerSeen = (provider: string, fileId: number) => {
      const bucket = seenByProvider.get(provider) ?? new Set<number>();
      bucket.add(fileId);
      seenByProvider.set(provider, bucket);
    };

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((provider: string) => ({
        id: provider,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer: vi.fn(async (pcmBuffer: Buffer) => {
          const fileId = Number.parseInt(pcmBuffer.toString('utf-8'), 10);
          registerSeen(provider, fileId);
          if (provider === 'openai') {
            const attempt = (openaiAttempts.get(fileId) ?? 0) + 1;
            openaiAttempts.set(fileId, attempt);
            if (fileId % 10 === 0 && attempt === 1) {
              throw new Error('timeout contacting provider');
            }
          }
          return { provider, text: `${provider}:${fileId}`, durationSec: 1 };
        }),
        transcribeFileFromPCM: vi.fn(async () => ({ provider, text: 'unused', durationSec: 1 })),
      })),
    }));

    const [{ BatchRunner }, { JobHistory }, { PersistentBatchJobStore }, { createStorage }] = await Promise.all([
      import('./batchRunner.js'),
      import('./jobHistory.js'),
      import('./persistentBatchJobStore.js'),
      import('../storage/index.js'),
    ]);

    const storage = createStorage('jsonl', storagePath);
    const history = new JobHistory(storage);
    await history.init();

    const jobStore = new PersistentBatchJobStore(path.join(tempDir, 'batch-jobs.sqlite'));
    const runner = new BatchRunner(storage, history, undefined, jobStore);
    await runner.init();

    const { jobId } = await runner.enqueue(['mock', 'openai'], 'ja-JP', fileInputs);
    const status = await waitForJobCompletion(runner, jobId, 20_000);
    expect(status).toMatchObject({
      jobId,
      total: 200,
      done: 200,
      failed: 0,
      status: 'completed',
    });

    const results = await runner.getResults(jobId);
    expect(results).not.toBeNull();
    expect(results).toHaveLength(200);
    expect(
      (results ?? []).every(
        (row) =>
          row.audioSpec?.sampleRateHz === 16000 &&
          row.audioSpec?.channels === 1 &&
          row.audioSpec?.encoding === 'linear16'
      )
    ).toBe(true);

    const expectedIds = new Set(Array.from({ length: 100 }, (_, index) => index));
    const mockSeen = seenByProvider.get('mock') ?? new Set<number>();
    const openaiSeen = seenByProvider.get('openai') ?? new Set<number>();
    expect(mockSeen.size).toBe(100);
    expect(openaiSeen.size).toBe(100);
    for (const fileId of expectedIds) {
      expect(mockSeen.has(fileId)).toBe(true);
      expect(openaiSeen.has(fileId)).toBe(true);
      const attempts = openaiAttempts.get(fileId) ?? 0;
      expect(attempts).toBe(fileId % 10 === 0 ? 2 : 1);
    }

    const persisted = await jobStore.getJob(jobId);
    expect(persisted).toMatchObject({
      jobId,
      total: 200,
      done: 200,
      failed: 0,
      status: 'completed',
    });
    const persistedRows = typeof storage.readByJob === 'function' ? await storage.readByJob(jobId) : [];
    expect(persistedRows).toHaveLength(200);
  });
});
