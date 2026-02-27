import { describe, it, expect } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { JsonlStore } from './jsonlStore.js';

describe('JsonlStore pruning', () => {
  it('drops records older than retention and caps maxRows', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'jsonl-prune-'));
    const store = new JsonlStore<any>(path.join(dir, 'test.jsonl'), {
      retentionMs: 5_000,
      maxRows: 2,
      pruneIntervalMs: 0,
    });

    await store.init();

    await store.append({ id: 'old', createdAt: new Date(Date.now() - 10_000).toISOString() });
    await store.append({ id: 'mid', createdAt: new Date().toISOString() });
    await store.append({ id: 'new', createdAt: new Date().toISOString() });

    const rows = await store.readAll();
    const ids = rows.map((r: any) => r.id);
    expect(ids).toEqual(['mid', 'new']);

    await rm(dir, { recursive: true, force: true });
  });

  it('uses recordedAt when present for retention', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'jsonl-prune-recorded-'));
    const store = new JsonlStore<any>(path.join(dir, 'test.jsonl'), {
      retentionMs: 5_000,
      pruneIntervalMs: 0,
    });

    await store.init();

    await store.append({ id: 'old', recordedAt: new Date(Date.now() - 10_000).toISOString() });
    await store.append({ id: 'new', recordedAt: new Date().toISOString() });

    const rows = await store.readAll();
    const ids = rows.map((r: any) => r.id);
    expect(ids).toEqual(['new']);

    await rm(dir, { recursive: true, force: true });
  });

  it('supports readByJob/readRecent/listJobIds without loading full history into caller', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'jsonl-read-by-job-'));
    const store = new JsonlStore<any>(path.join(dir, 'test.jsonl'));
    await store.init();

    await store.append({ id: 'a1', jobId: 'job-a', createdAt: new Date().toISOString() });
    await store.append({ id: 'b1', jobId: 'job-b', createdAt: new Date().toISOString() });
    await store.append({ id: 'a2', jobId: 'job-a', createdAt: new Date().toISOString() });

    const byJob = await store.readByJob('job-a');
    expect(byJob.map((row: any) => row.id)).toEqual(['a2', 'a1']);

    const recent = await store.readRecent(2);
    expect(recent.map((row: any) => row.id)).toEqual(['a2', 'b1']);

    const jobIds = await store.listJobIds();
    expect(new Set(jobIds)).toEqual(new Set(['job-a', 'job-b']));

    await rm(dir, { recursive: true, force: true });
  });
});
