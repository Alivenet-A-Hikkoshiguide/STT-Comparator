import { text as streamToText } from 'node:stream/consumers';
import { describe, expect, it } from 'vitest';
import type { BatchJobFileResult } from '../types.js';
import { toCsv, toCsvStream } from './csvExporter.js';

const row = (overrides: Partial<BatchJobFileResult> = {}): BatchJobFileResult => ({
  jobId: 'job-1',
  path: 'a.wav',
  provider: 'mock',
  lang: 'ja-JP',
  durationSec: 1,
  processingTimeMs: 100,
  rtf: 0.1,
  latencyMs: 100,
  text: 'hello',
  createdAt: '2026-02-25T00:00:00.000Z',
  ...overrides,
});

describe('csvExporter', () => {
  it('serializes csv deterministically', () => {
    const csv = toCsv([row({ text: 'hello, "csv"' })]);
    expect(csv).toContain(
      'path,provider,lang,cer,wer,rtf,latency_ms,audio_sample_rate_hz,audio_channels,audio_encoding,degraded,normalization,text,ref_text'
    );
    expect(csv).toContain('"hello, ""csv"""');
  });

  it('streams csv rows incrementally', async () => {
    const stream = toCsvStream([row({ path: 'a.wav' }), row({ path: 'b.wav' })]);
    const payload = await streamToText(stream);
    const lines = payload.trim().split('\n');
    expect(lines[0]).toContain('path,provider,lang');
    expect(lines).toHaveLength(3);
    expect(lines[2]).toContain('b.wav');
  });
});
