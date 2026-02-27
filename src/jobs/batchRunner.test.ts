import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { PassThrough } from 'node:stream';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { BatchJobFileResult, StorageDriver } from '../types.js';
import { JobHistory } from './jobHistory.js';
import { summarizeJob } from '../utils/summary.js';

vi.mock('../config.js', () => ({
  loadConfig: vi.fn().mockResolvedValue({
    audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
    ingressNormalize: { enabled: true },
    normalization: {},
    storage: { driver: 'jsonl', path: './runs' },
    providers: ['mock'],
    jobs: {
      maxParallel: 4,
      retentionMs: 10 * 60 * 1000,
      retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 10 },
    },
  }),
}));

describe('BatchRunner', () => {
  const memoryStore = (): StorageDriver<BatchJobFileResult> & { records: BatchJobFileResult[] } => {
    const records: BatchJobFileResult[] = [];
    return {
      records,
      init: async () => {},
      append: async (r: BatchJobFileResult) => {
        records.push(r);
      },
      readAll: async () => records,
    };
  };

  const setupRunner = async (
    store: ReturnType<typeof memoryStore>,
    providerReporter?: {
      recordProviderSuccess: (provider: string) => void;
      recordProviderFailure: (provider: string, reason?: string) => void;
    }
  ) => {
    const history = new JobHistory(store);
    await history.init();
    const { BatchRunner } = await import('./batchRunner.js');
    const runner = new BatchRunner(store, history, undefined, undefined, providerReporter as any);
    await runner.init();
    return { runner, store };
  };

  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  const waitForJob = async (
    runner: { getStatus: (id: string) => any },
    jobId: string,
    timeoutMs = 500
  ) => {
    const start = Date.now();
    let lastStatus: any = null;
    while (Date.now() - start < timeoutMs) {
      const status = runner.getStatus(jobId);
      lastStatus = status;
      if (status && (status.done + status.failed >= status.total)) return status;
      await new Promise((r) => setTimeout(r, 10));
    }
    throw new Error(`job did not complete in time; last status: ${JSON.stringify(lastStatus)}`);
  };

  it('falls back to measured duration when adapter does not provide durationSec', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'a.wav');
    await writeFile(filePath, Buffer.from('a'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 2,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end(Buffer.from([0, 1, 2, 3]));
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(2),
        })),
      };
    });

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'hi' })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const jobSpy = vi.spyOn(runner as any, 'processJob');
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [{ path: filePath, originalname: 'a.wav', size: 1 }]);

    await waitForJob(runner as any, jobId);

    expect(jobSpy).toHaveBeenCalled();

    expect(store.records).toHaveLength(1);
    expect(store.records[0].durationSec).toBeCloseTo(2, 6);
  });

  it('ignores adapter durationSec=0 and uses measured duration', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'a0.wav');
    await writeFile(filePath, Buffer.from('a'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 2,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end(Buffer.from([0, 1, 2, 3]));
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(2),
        })),
      };
    });

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'hi', durationSec: 0 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [
      { path: filePath, originalname: 'a0.wav', size: 1 },
    ]);

    await waitForJob(runner as any, jobId);

    expect(store.records).toHaveLength(1);
    expect(store.records[0].durationSec).toBeCloseTo(2, 6);
  });

  it('marks file as failed when manifest mapping is missing', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'b.wav');
    await writeFile(filePath, Buffer.from('b'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end();
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(1),
        })),
      };
    });

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'hi', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock'],
      'ja-JP',
      [{ path: filePath, originalname: 'b.wav', size: 1 }],
      { version: 1, language: 'ja-JP', items: [{ audio: 'a.wav', ref: 'ref' }] }
    );

    const status = await waitForJob(runner as any, jobId);

    expect(store.records).toHaveLength(0);
    expect(status?.failed).toBe(1);
    expect(status?.errors?.[0]?.message).toContain('manifest');
  });

  it('keeps total count after files array is cleared', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePathA = path.join(tmp, 'a.wav');
    const filePathB = path.join(tmp, 'b.wav');
    await writeFile(filePathA, Buffer.from('a'));
    await writeFile(filePathB, Buffer.from('b'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end(Buffer.from('a'));
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(1),
        })),
      };
    });

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'hi', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [
      { path: filePathA, originalname: 'a.wav', size: 1 },
      { path: filePathB, originalname: 'b.wav', size: 1 },
    ]);

    const status = await waitForJob(runner as any, jobId);
    expect(status?.total).toBe(2);
    expect((status?.done ?? 0) + (status?.failed ?? 0)).toBe(2);
  });

  it('processes all selected providers for each file', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'c.wav');
    await writeFile(filePath, Buffer.from('c'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end(Buffer.from('a'));
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(1),
        })),
      };
    });

    const transcribe = vi.fn(async (_pcm: any, _opts: any) => ({ provider: 'mock', text: 'ok', durationSec: 1 }));
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: transcribe,
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock', 'deepgram'], 'ja-JP', [
      { path: filePath, originalname: 'c.wav', size: 1 },
    ]);

    const status = await waitForJob(runner as any, jobId);
    expect(status?.total).toBe(2);
    expect(status?.done).toBe(2);
    expect(store.records).toHaveLength(2);
    expect(new Set(store.records.map((r) => r.provider))).toEqual(new Set(['mock', 'deepgram']));
    expect(transcribe).toHaveBeenCalledTimes(2);
  });

  it('reuses a single normalized PCM buffer and identical options across providers for each file', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'shared.wav');
    await writeFile(filePath, Buffer.from('shared'));

    const ensureNormalizedAudioMock = vi.fn(async (p: string) => ({
      normalizedPath: p,
      durationSec: 1,
      bytes: 4,
      degraded: false,
      generated: false,
      signature: 'sig',
      release: async () => {},
    }));
    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: ensureNormalizedAudioMock,
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async () => {
        const stream = new PassThrough();
        stream.end(Buffer.from([0, 1, 2, 3]));
        return { stream, durationPromise: Promise.resolve(1) };
      }),
    }));

    const invocations: Array<{ provider: string; pcm: Buffer; opts: unknown }> = [];
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer: vi.fn(async (pcm: Buffer, opts: unknown) => {
          invocations.push({ provider: id, pcm, opts });
          return { provider: id, text: 'ok', durationSec: 1 };
        }),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: id, text: 'unused', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock', 'deepgram'],
      'ja-JP',
      [{ path: filePath, originalname: 'shared.wav', size: 1 }],
      undefined,
      { punctuationPolicy: 'full', dictionaryPhrases: ['alpha'] }
    );

    const status = await waitForJob(runner as any, jobId, 1000);
    expect(status?.done).toBe(2);
    expect(ensureNormalizedAudioMock).toHaveBeenCalledTimes(1);
    expect(invocations).toHaveLength(2);
    expect(invocations[0]?.pcm).toBe(invocations[1]?.pcm);
    expect(invocations[0]?.opts).toEqual(invocations[1]?.opts);
    expect(invocations[0]?.opts).toEqual(
      expect.objectContaining({
        language: 'ja-JP',
        sampleRateHz: 16000,
        encoding: 'linear16',
        punctuationPolicy: 'full',
        dictionaryPhrases: ['alpha'],
      })
    );
  });

  it('honors manifest stripSpace normalization for non-japanese scoring', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'space.wav');
    await writeFile(filePath, Buffer.from('s'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => {
      const stream = new PassThrough();
      stream.end(Buffer.from('a'));
      return {
        convertToPcmReadable: vi.fn(async () => ({
          stream,
          durationPromise: Promise.resolve(1),
        })),
      };
    });

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'a b', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock'],
      'en-US',
      [{ path: filePath, originalname: 'space.wav', size: 1 }],
      {
        version: 1,
        language: 'en-US',
        items: [{ audio: 'space.wav', ref: 'ab' }],
        normalization: { stripSpace: true },
      }
    );

    const status = await waitForJob(runner as any, jobId);
    expect(status?.done).toBe(1);
    expect(store.records).toHaveLength(1);
    expect(store.records[0].normalizationUsed?.stripSpace).toBe(true);
    expect(store.records[0].wer).toBe(0);
  });

  it('releases normalized audio when PCM conversion fails during prepare', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'd.wav');
    await writeFile(filePath, Buffer.from('d'));

    const release = vi.fn(async () => {});
    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release,
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async () => {
        throw new Error('ffmpeg fail');
      }),
    }));

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'hi', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [{ path: filePath, originalname: 'd.wav', size: 1 }]);

    const status = await waitForJob(runner as any, jobId);
    expect(status?.failed).toBe(1);
    expect(release).toHaveBeenCalledTimes(1);
    expect(store.records).toHaveLength(0);
  });

  it('does not keep in-memory results when storage append fails', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'e.wav');
    await writeFile(filePath, Buffer.from('e'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'hi', durationSec: 1 })),
      })),
    }));

    const records: BatchJobFileResult[] = [];
    const store: StorageDriver<BatchJobFileResult> & { records: BatchJobFileResult[] } = {
      records,
      init: async () => {},
      append: async () => {
        throw new Error('append failed');
      },
      readAll: async () => records,
    };
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [{ path: filePath, originalname: 'e.wav', size: 1 }]);

    const status = await waitForJob(runner as any, jobId);
    expect(status?.done).toBe(0);
    expect(status?.failed).toBe(1);
    expect(status?.errors?.[0]?.message).toContain('append failed');
    expect(store.records).toHaveLength(0);

    const results = await (runner as any).getResults(jobId);
    expect(results).toHaveLength(0);
  });

  it('retries transient adapter errors and succeeds', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'retry.wav');
    await writeFile(filePath, Buffer.from('r'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    const transcribe = vi
      .fn()
      .mockRejectedValueOnce(new Error('timeout contacting provider'))
      .mockResolvedValue({ provider: 'mock', text: 'ok', durationSec: 1 });
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: transcribe,
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [
      { path: filePath, originalname: 'retry.wav', size: 1 },
    ]);

    const status = await waitForJob(runner as any, jobId, 2000);
    expect(status?.done).toBe(1);
    expect(status?.failed).toBe(0);
    expect(transcribe).toHaveBeenCalledTimes(2);
    expect(store.records).toHaveLength(1);
  });

  it('uses transcribePcmBuffer when adapter provides it', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'buffer.wav');
    await writeFile(filePath, Buffer.from('b'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    const transcribePcmBuffer = vi.fn(async () => ({ provider: 'mock', text: 'buffer-ok', durationSec: 1 }));
    const transcribeFileFromPCM = vi.fn(async () => ({ provider: 'mock', text: 'stream-path', durationSec: 1 }));
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer,
        transcribeFileFromPCM,
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [
      { path: filePath, originalname: 'buffer.wav', size: 1 },
    ]);

    const status = await waitForJob(runner as any, jobId, 1000);
    expect(status?.done).toBe(1);
    expect(status?.failed).toBe(0);
    expect(transcribePcmBuffer).toHaveBeenCalledTimes(1);
    expect(transcribeFileFromPCM).not.toHaveBeenCalled();
    expect(store.records[0]?.text).toBe('buffer-ok');
  });

  it('reports provider runtime success/failure to availability cache hooks', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePathSuccess = path.join(tmp, 'ok.wav');
    const filePathFail = path.join(tmp, 'fail.wav');
    await writeFile(filePathSuccess, Buffer.from('s'));
    await writeFile(filePathFail, Buffer.from('f'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async () => {
        const stream = new PassThrough();
        stream.end(Buffer.from([0, 1, 2, 3]));
        return { stream, durationPromise: Promise.resolve(1) };
      }),
    }));

    const transcribePcmBuffer = vi
      .fn()
      .mockResolvedValueOnce({ provider: 'mock', text: 'ok', durationSec: 1 })
      .mockImplementation(async () => {
        throw new Error('provider timeout');
      });
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer,
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'unused', durationSec: 1 })),
      })),
    }));

    const reporter = {
      recordProviderSuccess: vi.fn(),
      recordProviderFailure: vi.fn(),
    };
    const store = memoryStore();
    const { runner } = await setupRunner(store, reporter);
    const { jobId } = await runner.enqueue(['mock'], 'ja-JP', [
      { path: filePathSuccess, originalname: 'ok.wav', size: 1 },
      { path: filePathFail, originalname: 'fail.wav', size: 1 },
    ]);

    const status = await waitForJob(runner as any, jobId, 3000);
    expect(status?.done).toBe(1);
    expect(status?.failed).toBe(1);
    expect(reporter.recordProviderSuccess).toHaveBeenCalledTimes(1);
    expect(reporter.recordProviderSuccess).toHaveBeenNthCalledWith(1, 'mock');
    expect(reporter.recordProviderFailure).toHaveBeenCalledTimes(1);
    expect(reporter.recordProviderFailure).toHaveBeenNthCalledWith(
      1,
      'mock',
      expect.stringContaining('provider timeout')
    );
  });

  it('emits a warning when requested parallel slots are clamped', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'clamp.wav');
    await writeFile(filePath, Buffer.from('c'));

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock', 'deepgram'],
      jobs: {
        maxParallel: 2,
        retentionMs: 10 * 60 * 1000,
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: id, text: 'ok', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock', 'deepgram'],
      'ja-JP',
      [{ path: filePath, originalname: 'clamp.wav', size: 1 }],
      undefined,
      { parallel: 2 }
    );

    const status = await waitForJob(runner as any, jobId, 1000);
    expect(status?.done).toBe(2);
    expect(status?.warnings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: 'parallel_clamped',
        }),
      ])
    );
  });

  it('applies providerMaxParallel limits across files', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePaths = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `limit-${index}.wav`);
        await writeFile(filePath, Buffer.from(`f${index}`));
        return filePath;
      })
    );

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock', 'deepgram'],
      jobs: {
        maxParallel: 8,
        retentionMs: 10 * 60 * 1000,
        providerMaxParallel: { mock: 1, deepgram: 2 },
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    const activeByProvider: Record<string, number> = { mock: 0, deepgram: 0 };
    const maxActiveByProvider: Record<string, number> = { mock: 0, deepgram: 0 };
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => {
          activeByProvider[id] = (activeByProvider[id] ?? 0) + 1;
          maxActiveByProvider[id] = Math.max(
            maxActiveByProvider[id] ?? 0,
            activeByProvider[id] ?? 0
          );
          await new Promise((resolve) => setTimeout(resolve, 20));
          activeByProvider[id] = Math.max(0, (activeByProvider[id] ?? 1) - 1);
          return { provider: id, text: 'ok', durationSec: 1 };
        }),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock', 'deepgram'],
      'ja-JP',
      filePaths.map((filePath, index) => ({
        path: filePath,
        originalname: `limit-${index}.wav`,
        size: 1,
      })),
      undefined,
      { parallel: 4 }
    );

    const status = await waitForJob(runner as any, jobId, 3000);
    expect(status?.done).toBe(8);
    expect(maxActiveByProvider.mock).toBeLessThanOrEqual(1);
    expect(maxActiveByProvider.deepgram).toBeLessThanOrEqual(2);
  });

  it('enforces providerMaxParallel globally across concurrently running jobs', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filesJobA = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `provider-cap-a-${index}.wav`);
        await writeFile(filePath, Buffer.from(`a${index}`));
        return filePath;
      })
    );
    const filesJobB = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `provider-cap-b-${index}.wav`);
        await writeFile(filePath, Buffer.from(`b${index}`));
        return filePath;
      })
    );

    vi.doMock('node:os', () => ({
      default: {
        cpus: () => Array.from({ length: 8 }).map(() => ({})),
      },
    }));

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock'],
      jobs: {
        maxParallel: 8,
        retentionMs: 10 * 60 * 1000,
        providerMaxParallel: { mock: 1 },
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async () => {
        const stream = new PassThrough();
        stream.end(Buffer.from([0, 1, 2, 3]));
        return { stream, durationPromise: Promise.resolve(1) };
      }),
    }));

    let active = 0;
    let maxActive = 0;
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer: vi.fn(async () => {
          active += 1;
          maxActive = Math.max(maxActive, active);
          await new Promise((resolve) => setTimeout(resolve, 20));
          active = Math.max(0, active - 1);
          return { provider: id, text: 'ok', durationSec: 1 };
        }),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: id, text: 'ok', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);

    const [{ jobId: jobIdA }, { jobId: jobIdB }] = await Promise.all([
      runner.enqueue(
        ['mock'],
        'ja-JP',
        filesJobA.map((filePath, index) => ({
          path: filePath,
          originalname: `provider-cap-a-${index}.wav`,
          size: 1,
        })),
        undefined,
        { parallel: 4 }
      ),
      runner.enqueue(
        ['mock'],
        'ja-JP',
        filesJobB.map((filePath, index) => ({
          path: filePath,
          originalname: `provider-cap-b-${index}.wav`,
          size: 1,
        })),
        undefined,
        { parallel: 4 }
      ),
    ]);

    const [statusA, statusB] = await Promise.all([
      waitForJob(runner as any, jobIdA, 4000),
      waitForJob(runner as any, jobIdB, 4000),
    ]);

    expect(statusA?.done).toBe(4);
    expect(statusB?.done).toBe(4);
    expect(maxActive).toBeLessThanOrEqual(1);
  });

  it('rotates provider start order per file when provider concurrency is clamped', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePaths = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `rotate-${index}.wav`);
        await writeFile(filePath, Buffer.from(`r${index}`));
        return filePath;
      })
    );

    vi.doMock('node:os', () => ({
      default: {
        cpus: () => Array.from({ length: 8 }).map(() => ({})),
      },
    }));

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock', 'deepgram'],
      jobs: {
        maxParallel: 1,
        retentionMs: 10 * 60 * 1000,
        providerMaxParallel: { mock: 1, deepgram: 1 },
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async (source: { path?: string }) => {
        const stagedPath = String(source.path ?? '');
        const basename = path.basename(stagedPath);
        const match = basename.match(/^(\d+)-/);
        const fileIndex = Number(match?.[1] ?? 0);
        const stream = new PassThrough();
        stream.end(Buffer.from([fileIndex]));
        return { stream, durationPromise: Promise.resolve(1) };
      }),
    }));

    const firstProviderByFile = new Map<number, string>();
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer: vi.fn(async (pcm: Buffer) => {
          const fileIndex = Number(pcm[0] ?? 0);
          if (!firstProviderByFile.has(fileIndex)) {
            firstProviderByFile.set(fileIndex, id);
          }
          return { provider: id, text: 'ok', durationSec: 1 };
        }),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: id, text: 'ok', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock', 'deepgram'],
      'ja-JP',
      filePaths.map((filePath, index) => ({
        path: filePath,
        originalname: `rotate-${index}.wav`,
        size: 1,
      })),
      undefined,
      { parallel: 1 }
    );

    const status = await waitForJob(runner as any, jobId, 3000);
    expect(status?.done).toBe(8);
    expect(firstProviderByFile.size).toBe(4);
    const firstCounts = Array.from(firstProviderByFile.values()).reduce<Record<string, number>>((acc, provider) => {
      acc[provider] = (acc[provider] ?? 0) + 1;
      return acc;
    }, {});
    expect(firstCounts.mock).toBe(2);
    expect(firstCounts.deepgram).toBe(2);
  });

  it('scales provider concurrency when options.parallel is feasible', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePaths = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `scale-${index}.wav`);
        await writeFile(filePath, Buffer.from(`f${index}`));
        return filePath;
      })
    );

    vi.doMock('node:os', () => ({
      default: {
        cpus: () => Array.from({ length: 8 }).map(() => ({})),
      },
    }));

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock', 'deepgram'],
      jobs: {
        maxParallel: 8,
        retentionMs: 10 * 60 * 1000,
        providerMaxParallel: { mock: 4, deepgram: 4 },
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    const activeByProvider: Record<string, number> = { mock: 0, deepgram: 0 };
    const maxActiveByProvider: Record<string, number> = { mock: 0, deepgram: 0 };
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => {
          activeByProvider[id] = (activeByProvider[id] ?? 0) + 1;
          maxActiveByProvider[id] = Math.max(maxActiveByProvider[id] ?? 0, activeByProvider[id] ?? 0);
          await new Promise((resolve) => setTimeout(resolve, 20));
          activeByProvider[id] = Math.max(0, (activeByProvider[id] ?? 1) - 1);
          return { provider: id, text: 'ok', durationSec: 1 };
        }),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock', 'deepgram'],
      'ja-JP',
      filePaths.map((filePath, index) => ({
        path: filePath,
        originalname: `scale-${index}.wav`,
        size: 1,
      })),
      undefined,
      { parallel: 2 }
    );

    const status = await waitForJob(runner as any, jobId, 3000);
    expect(status?.done).toBe(8);
    expect(maxActiveByProvider.mock).toBeGreaterThanOrEqual(2);
    expect(maxActiveByProvider.deepgram).toBeGreaterThanOrEqual(2);
    expect(status?.warnings.some((warning: { code: string }) => warning.code === 'parallel_clamped')).toBe(false);
  });

  it('enforces maxParallel globally across concurrently running jobs', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filesJobA = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `job-a-${index}.wav`);
        await writeFile(filePath, Buffer.from(`a${index}`));
        return filePath;
      })
    );
    const filesJobB = await Promise.all(
      Array.from({ length: 4 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `job-b-${index}.wav`);
        await writeFile(filePath, Buffer.from(`b${index}`));
        return filePath;
      })
    );

    vi.doMock('node:os', () => ({
      default: {
        cpus: () => Array.from({ length: 8 }).map(() => ({})),
      },
    }));

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock'],
      jobs: {
        maxParallel: 2,
        retentionMs: 10 * 60 * 1000,
        providerMaxParallel: { mock: 4 },
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
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

    let active = 0;
    let maxActive = 0;
    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => {
          active += 1;
          maxActive = Math.max(maxActive, active);
          await new Promise((resolve) => setTimeout(resolve, 25));
          active = Math.max(0, active - 1);
          return { provider: id, text: 'ok', durationSec: 1 };
        }),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);

    const [{ jobId: jobIdA }, { jobId: jobIdB }] = await Promise.all([
      runner.enqueue(
        ['mock'],
        'ja-JP',
        filesJobA.map((filePath, index) => ({
          path: filePath,
          originalname: `job-a-${index}.wav`,
          size: 1,
        })),
        undefined,
        { parallel: 2 }
      ),
      runner.enqueue(
        ['mock'],
        'ja-JP',
        filesJobB.map((filePath, index) => ({
          path: filePath,
          originalname: `job-b-${index}.wav`,
          size: 1,
        })),
        undefined,
        { parallel: 2 }
      ),
    ]);

    const [statusA, statusB] = await Promise.all([
      waitForJob(runner as any, jobIdA, 5000),
      waitForJob(runner as any, jobIdB, 5000),
    ]);
    expect(statusA?.done).toBe(4);
    expect(statusB?.done).toBe(4);
    expect(maxActive).toBeLessThanOrEqual(2);
  });

  it('emits a warning when degraded normalization is present in results', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePath = path.join(tmp, 'degraded.wav');
    await writeFile(filePath, Buffer.from('d'));

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 1,
        bytes: 4,
        degraded: true,
        generated: true,
        signature: 'sig',
        release: async () => {},
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

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn(() => ({
        id: 'mock',
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: 'mock', text: 'ok', durationSec: 1 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock'],
      'ja-JP',
      [{ path: filePath, originalname: 'degraded.wav', size: 1 }]
    );

    const status = await waitForJob(runner as any, jobId, 1000);
    expect(status?.done).toBe(1);
    expect(status?.warnings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: 'degraded_audio_detected',
          details: expect.objectContaining({
            degradedCount: 1,
          }),
        }),
      ])
    );
  });

  it('keeps fair-input and retry behavior consistent for 100 files x 2 providers', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const filePaths = await Promise.all(
      Array.from({ length: 100 }).map(async (_item, index) => {
        const filePath = path.join(tmp, `perf-${index}.wav`);
        await writeFile(filePath, Buffer.from(String(index), 'utf-8'));
        return filePath;
      })
    );

    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock', 'deepgram'],
      jobs: {
        maxParallel: 16,
        retentionMs: 10 * 60 * 1000,
        providerMaxParallel: { mock: 8, deepgram: 8 },
        retry: { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    vi.doMock('../utils/audioIngress.js', () => ({
      ensureNormalizedAudio: vi.fn(async (p: string) => ({
        normalizedPath: p,
        durationSec: 30,
        bytes: 4,
        degraded: false,
        generated: false,
        signature: 'sig',
        release: async () => {},
      })),
      AudioValidationError: class extends Error {},
    }));

    vi.doMock('../utils/ffmpeg.js', () => ({
      convertToPcmReadable: vi.fn(async (source: NodeJS.ReadableStream) => {
        return { stream: source, durationPromise: Promise.resolve(30) };
      }),
    }));

    const deepgramAttempts = new Map<number, number>();
    const seenByProvider = new Map<string, Set<number>>();
    const registerSeen = (provider: string, fileId: number) => {
      const bucket = seenByProvider.get(provider) ?? new Set<number>();
      bucket.add(fileId);
      seenByProvider.set(provider, bucket);
    };

    vi.doMock('../adapters/index.js', () => ({
      getAdapter: vi.fn((id: string) => ({
        id,
        supportsStreaming: true,
        supportsBatch: true,
        startStreaming: vi.fn(),
        transcribePcmBuffer: vi.fn(async (pcmBuffer: Buffer) => {
          const fileId = Number.parseInt(pcmBuffer.toString('utf-8'), 10);
          registerSeen(id, fileId);
          if (id === 'deepgram') {
            const attempt = (deepgramAttempts.get(fileId) ?? 0) + 1;
            deepgramAttempts.set(fileId, attempt);
            if (fileId % 10 === 0 && attempt === 1) {
              throw new Error('timeout contacting provider');
            }
          }
          return { provider: id, text: 'ok', durationSec: 30 };
        }),
        transcribeFileFromPCM: vi.fn(async () => ({ provider: id, text: 'ok', durationSec: 30 })),
      })),
    }));

    const store = memoryStore();
    const { runner } = await setupRunner(store);
    const { jobId } = await runner.enqueue(
      ['mock', 'deepgram'],
      'ja-JP',
      filePaths.map((filePath, index) => ({
        path: filePath,
        originalname: `perf-${index}.wav`,
        size: 1,
      })),
      undefined,
      { parallel: 4 }
    );

    const status = await waitForJob(runner as any, jobId, 15_000);
    expect(status?.failed).toBe(0);
    expect(status?.done).toBe(200);

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

    const summary = summarizeJob(results ?? []);
    expect(summary.count).toBe(200);
    expect(summary.rtf.avg).not.toBeNull();
    expect(summary.latencyMs.p95).not.toBeNull();
    expect(summary.rtf.avg ?? Infinity).toBeLessThanOrEqual(1.0);
    expect(summary.latencyMs.p95 ?? Infinity).toBeLessThanOrEqual(1500);

    const mockSeen = seenByProvider.get('mock') ?? new Set<number>();
    const deepgramSeen = seenByProvider.get('deepgram') ?? new Set<number>();
    expect(mockSeen.size).toBe(100);
    expect(deepgramSeen.size).toBe(100);
    for (let fileId = 0; fileId < 100; fileId += 1) {
      expect(mockSeen.has(fileId)).toBe(true);
      expect(deepgramSeen.has(fileId)).toBe(true);
      expect(deepgramAttempts.get(fileId)).toBe(fileId % 10 === 0 ? 2 : 1);
    }
  });

  it('fails init when persistent job store is unavailable in required mode', async () => {
    const tmp = await mkdtemp(path.join(tmpdir(), 'batch-test-'));
    const { loadConfig } = await import('../config.js');
    vi.mocked(loadConfig).mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true },
      normalization: {},
      storage: { driver: 'jsonl', path: tmp },
      providers: ['mock'],
      jobs: {
        maxParallel: 4,
        retentionMs: 10 * 60 * 1000,
        persistenceMode: 'required',
        retry: { maxAttempts: 1, baseDelayMs: 1, maxDelayMs: 5 },
      },
    } as any);

    const store = memoryStore();
    const history = new JobHistory(store);
    await history.init();
    const { BatchRunner } = await import('./batchRunner.js');
    const failingJobStore = {
      init: vi.fn(async () => {
        throw new Error('sqlite unavailable');
      }),
    } as any;
    const runner = new BatchRunner(store, history, undefined, failingJobStore);

    await expect(runner.init()).rejects.toThrow('persistent batch job store initialization failed');
  });

});
