import { describe, expect, it } from 'vitest';
import type { BatchJobFileResult } from '../types.js';
import { summarizeJob, summarizeJobByProvider } from './summary.js';

const row = (overrides: Partial<BatchJobFileResult> = {}): BatchJobFileResult => ({
  jobId: 'job-1',
  path: 'a.wav',
  provider: 'mock',
  lang: 'ja-JP',
  durationSec: 1,
  processingTimeMs: 100,
  rtf: 0.1,
  latencyMs: 100,
  text: 'a',
  createdAt: '2026-02-25T00:00:00.000Z',
  ...overrides,
});

describe('summary', () => {
  it('computes avg/p50/p95 from finite values only', () => {
    const summary = summarizeJob([
      row({ cer: 0.3, wer: 0.2, rtf: 1, latencyMs: 100 }),
      row({ cer: 0.1, wer: undefined, rtf: 2, latencyMs: 200, degraded: true }),
      row({ cer: Number.NaN, wer: 0.4, rtf: 3, latencyMs: 300 }),
    ]);

    expect(summary.count).toBe(3);
    expect(summary.cer.n).toBe(2);
    expect(summary.cer.avg).toBeCloseTo(0.2, 6);
    expect(summary.cer.p50).toBeCloseTo(0.2, 6);
    expect(summary.cer.p95).toBeCloseTo(0.29, 6);
    expect(summary.wer.n).toBe(2);
    expect(summary.rtf.p50).toBe(2);
    expect(summary.latencyMs.p95).toBeCloseTo(290, 6);
    expect(summary.degraded.count).toBe(1);
    expect(summary.degraded.ratio).toBeCloseTo(1 / 3, 6);
  });

  it('groups summaries by provider', () => {
    const grouped = summarizeJobByProvider([
      row({ provider: 'mock', latencyMs: 100 }),
      row({ provider: 'deepgram', latencyMs: 200 }),
      row({ provider: 'deepgram', latencyMs: 300 }),
    ]);

    expect(grouped.mock.count).toBe(1);
    expect(grouped.deepgram.count).toBe(2);
    expect(grouped.deepgram.latencyMs.p50).toBe(250);
    expect(grouped.deepgram.degraded.count).toBe(0);
  });
});
