import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import Database from 'better-sqlite3';
import { PersistentBatchJobStore } from './persistentBatchJobStore.js';

const tempDirs: string[] = [];
let sqliteAvailable = true;
try {
  const probe = new Database(':memory:');
  probe.close();
} catch {
  sqliteAvailable = false;
}
const maybeIt = sqliteAvailable ? it : it.skip;

const makeStore = async () => {
  const dir = await mkdtemp(path.join(tmpdir(), 'batch-job-store-'));
  tempDirs.push(dir);
  const store = new PersistentBatchJobStore(path.join(dir, 'batch-jobs.sqlite'));
  await store.init();
  return store;
};

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

describe('PersistentBatchJobStore', () => {
  maybeIt('persists task transitions and exposes aggregate status', async () => {
    const store = await makeStore();
    const jobId = 'job-status';

    await store.createJob({
      jobId,
      providers: ['mock', 'openai'],
      lang: 'ja-JP',
      files: [{ index: 0, originalname: 'a.wav', path: '/tmp/a.wav', size: 1 }],
    });
    await store.setJobStatus(jobId, 'running');
    await store.markTaskRunning(jobId, 0, 'mock');
    await store.markTaskDone(jobId, 0, 'mock');
    await store.markTaskRunning(jobId, 0, 'openai');
    await store.markTaskFailed(jobId, 0, 'openai', {
      file: 'a.wav (openai)',
      message: 'provider timeout',
    });
    await store.setJobStatus(jobId, 'completed');

    const status = await store.getJobStatus(jobId);
    expect(status).toMatchObject({
      jobId,
      total: 2,
      done: 1,
      failed: 1,
      status: 'completed',
    });
    expect(status?.providers).toEqual(['mock', 'openai']);
    expect(status?.errors).toEqual([{ file: 'a.wav (openai)', message: 'provider timeout' }]);
    expect(status?.warnings).toEqual([]);

    const job = await store.getJob(jobId);
    expect(job).not.toBeNull();
    const mockTask = job?.tasks.find((task) => task.provider === 'mock');
    const openaiTask = job?.tasks.find((task) => task.provider === 'openai');
    expect(mockTask).toMatchObject({ status: 'done', attempts: 1, lastError: undefined });
    expect(openaiTask).toMatchObject({
      status: 'failed',
      attempts: 1,
      lastError: 'provider timeout',
    });

    const activeJobs = await store.listActiveJobs();
    expect(activeJobs).toHaveLength(0);
  });

  maybeIt('resets running tasks back to pending for recovery', async () => {
    const store = await makeStore();
    const jobId = 'job-recover';

    await store.createJob({
      jobId,
      providers: ['mock'],
      lang: 'ja-JP',
      files: [
        { index: 0, originalname: 'a.wav', path: '/tmp/a.wav', size: 1 },
        { index: 1, originalname: 'b.wav', path: '/tmp/b.wav', size: 1 },
      ],
    });
    await store.setJobStatus(jobId, 'running');
    await store.markTaskRunning(jobId, 0, 'mock');

    const beforeReset = await store.listActiveJobs();
    expect(beforeReset).toHaveLength(1);
    expect(beforeReset[0]?.tasks.find((task) => task.fileIndex === 0)?.status).toBe('running');

    await store.resetRunningTasks(jobId);

    const reloaded = await store.getJob(jobId);
    expect(reloaded?.tasks.find((task) => task.fileIndex === 0)?.status).toBe('pending');
    expect(reloaded?.tasks.find((task) => task.fileIndex === 1)?.status).toBe('pending');
  });

  maybeIt('stores warnings without duplicates', async () => {
    const store = await makeStore();
    const jobId = 'job-warning';

    await store.createJob({
      jobId,
      providers: ['mock'],
      lang: 'ja-JP',
      files: [{ index: 0, originalname: 'a.wav', path: '/tmp/a.wav', size: 1 }],
    });

    const warning = {
      code: 'parallel_clamped' as const,
      message: 'parallelism clamped',
      createdAt: new Date().toISOString(),
      details: { maxParallel: 1 },
    };
    await store.addJobWarning(jobId, warning);
    await store.addJobWarning(jobId, warning);

    const status = await store.getJobStatus(jobId);
    expect(status?.warnings).toHaveLength(1);
    expect(status?.warnings[0]).toMatchObject({
      code: 'parallel_clamped',
      message: 'parallelism clamped',
    });
  });

  maybeIt('lists persisted jobs including failed-only jobs', async () => {
    const store = await makeStore();

    await store.createJob({
      jobId: 'job-failed',
      providers: ['mock'],
      lang: 'ja-JP',
      files: [{ index: 0, originalname: 'failed.wav', path: '/tmp/failed.wav', size: 1 }],
    });
    await store.setJobStatus('job-failed', 'running');
    await store.markTaskRunning('job-failed', 0, 'mock');
    await store.markTaskFailed('job-failed', 0, 'mock', {
      file: 'failed.wav (mock)',
      message: 'decode failed',
    });
    await store.setJobStatus('job-failed', 'failed');

    await store.createJob({
      jobId: 'job-success',
      providers: ['mock'],
      lang: 'ja-JP',
      files: [{ index: 0, originalname: 'ok.wav', path: '/tmp/ok.wav', size: 1 }],
    });
    await store.setJobStatus('job-success', 'running');
    await store.markTaskRunning('job-success', 0, 'mock');
    await store.markTaskDone('job-success', 0, 'mock');
    await store.setJobStatus('job-success', 'completed');

    const listed = await store.listJobs(10);
    const failed = listed.find((entry) => entry.jobId === 'job-failed');
    const success = listed.find((entry) => entry.jobId === 'job-success');

    expect(failed).toMatchObject({
      jobId: 'job-failed',
      total: 1,
      done: 0,
      failed: 1,
      status: 'failed',
    });
    expect(success).toMatchObject({
      jobId: 'job-success',
      total: 1,
      done: 1,
      failed: 0,
      status: 'completed',
    });
  });
});
