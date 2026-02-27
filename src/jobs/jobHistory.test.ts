import { describe, expect, it, vi } from 'vitest';
import { JobHistory } from './jobHistory.js';
import type { BatchJobFileResult } from '../types.js';

const baseResult = (overrides: Partial<BatchJobFileResult> = {}): BatchJobFileResult => ({
  jobId: 'job-a',
  path: 'foo.wav',
  provider: 'mock',
  lang: 'ja-JP',
  durationSec: 1,
  processingTimeMs: 1000,
  rtf: 1,
  text: 'hello',
  createdAt: '2025-11-25T01:00:00.000Z',
  ...overrides,
});

const memoryStorage = (
  records: BatchJobFileResult[],
  afterPrune?: BatchJobFileResult[]
) => {
  let calls = 0;
  const readAll = vi.fn().mockImplementation(() => {
    calls += 1;
    if (afterPrune && calls > 1) {
      return Promise.resolve(afterPrune);
    }
    return Promise.resolve(records);
  });
  const readByJob = vi.fn().mockImplementation((jobId: string) =>
    Promise.resolve(records.filter((row) => row.jobId === jobId))
  );

  return {
    append: vi.fn().mockResolvedValue(undefined),
    init: vi.fn().mockResolvedValue(undefined),
    readAll,
    readByJob,
  };
};

describe('JobHistory', () => {
  it('builds entries from storage and sorts by most recent update', async () => {
    const driver = memoryStorage([
      baseResult({ jobId: 'job-a', createdAt: '2025-11-25T01:00:00.000Z' }),
      baseResult({ jobId: 'job-a', createdAt: '2025-11-25T01:01:00.000Z' }),
      baseResult({ jobId: 'job-b', createdAt: '2025-11-25T02:00:00.000Z', provider: 'deepgram', lang: 'en-US' }),
    ]);

    const history = new JobHistory(driver, { syncIntervalMs: 0 });
    await history.init();

    const entries = await history.list();
    expect(entries.map((entry) => entry.jobId)).toEqual(['job-b', 'job-a']);
    expect(entries[0].summary.count).toBe(1);
    expect(entries[1].summary.count).toBe(2);
    expect(entries[1].createdAt).toBe('2025-11-25T01:00:00.000Z');
    expect(entries[1].updatedAt).toBe('2025-11-25T01:01:00.000Z');
  });

  it('drops jobs whose rows were pruned from storage', async () => {
    const driver = memoryStorage(
      [
        baseResult({ jobId: 'job-a', createdAt: '2025-11-25T01:00:00.000Z' }),
        baseResult({ jobId: 'job-b', createdAt: '2025-11-25T02:00:00.000Z' }),
      ],
      [baseResult({ jobId: 'job-a', createdAt: '2025-11-25T01:00:00.000Z' })]
    );

    const history = new JobHistory(driver, { syncIntervalMs: 0 });
    await history.init();

    const entries = await history.list();
    expect(entries.some((entry) => entry.jobId === 'job-b')).toBe(false);
    expect(entries.some((entry) => entry.jobId === 'job-a')).toBe(true);
  });
});
