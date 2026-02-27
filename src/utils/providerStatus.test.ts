import { describe, expect, beforeEach, it, vi } from 'vitest';

vi.mock('./whisper.js', () => ({
  getWhisperRuntime: vi.fn(),
  resetWhisperRuntimeCache: vi.fn(),
}));

vi.mock('ws', () => {
  const { EventEmitter } = require('node:events');
  const state = {
    instances: [] as any[],
    outcome: 'open' as 'open' | 'error' | 'close',
    error: null as Error | null,
  };
  class FakeWebSocket extends EventEmitter {
    url: string;
    terminate = vi.fn(() => this.emit('close'));
    close = vi.fn(() => this.emit('close'));
    constructor(url: string) {
      super();
      this.url = url;
      state.instances.push(this);
      queueMicrotask(() => {
        if (state.outcome === 'open') this.emit('open');
        else if (state.outcome === 'error') this.emit('error', state.error ?? new Error('ws error'));
        else this.emit('close');
      });
    }
  }
  return {
    WebSocket: FakeWebSocket,
    __setOutcome: (outcome: 'open' | 'error' | 'close', error?: Error) => {
      state.outcome = outcome;
      state.error = error ?? null;
    },
    __instances: state.instances,
  };
});

import { computeProviderAvailability } from './providerStatus.js';
import { getWhisperRuntime, resetWhisperRuntimeCache } from './whisper.js';
import type { AppConfig } from '../config.js';

const mockGetWhisperRuntime = getWhisperRuntime as unknown as ReturnType<typeof vi.fn>;
const mockReset = resetWhisperRuntimeCache as unknown as ReturnType<typeof vi.fn>;

const baseConfig: AppConfig = {
  audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
  normalization: {},
  storage: { driver: 'jsonl', path: './runs', retentionDays: 30, maxRows: 100000 },
  providers: ['local_whisper'],
  jobs: { maxParallel: 4, retentionMs: 10 * 60 * 1000, retry: { maxAttempts: 3, baseDelayMs: 1000, maxDelayMs: 10_000 } },
  ws: {},
  providerHealth: {},
  providerLimits: {},
};

describe('computeProviderAvailability(local_whisper)', () => {
  beforeEach(() => {
    mockGetWhisperRuntime.mockReset();
    mockReset.mockReset();
  });

  it('returns unavailable when whisper runtime is missing', async () => {
    mockGetWhisperRuntime.mockReturnValue({ pythonPath: null, reason: 'missing' });

    const result = await computeProviderAvailability(baseConfig);
    const lw = result.find((p) => p.id === 'local_whisper');

    expect(mockReset).toHaveBeenCalled();
    expect(lw).toBeDefined();
    expect(lw?.available).toBe(false);
    expect(lw?.supportsBatch).toBe(true);
    expect(lw?.supportsStreaming).toBe(false);
    expect(lw?.reason).toContain('missing');
  });

  it('returns available with batch-only support when runtime exists', async () => {
    mockGetWhisperRuntime.mockReturnValue({ pythonPath: '/tmp/python' });

    const result = await computeProviderAvailability(baseConfig);
    const lw = result.find((p) => p.id === 'local_whisper');

    expect(lw?.available).toBe(true);
    expect(lw?.supportsBatch).toBe(true);
    expect(lw?.supportsStreaming).toBe(false);
  });
});

describe('computeProviderAvailability(whisper_streaming)', () => {
  const createMonitor = (snapshot: { available: boolean; reason?: string }) => ({
    updateRefreshMs: vi.fn(),
    triggerCheck: vi.fn(),
    getSnapshot: vi.fn(() => snapshot),
    forceCheck: vi.fn(async () => {}),
  });

  it('propagates the monitor availability when healthy', async () => {
    const monitor = createMonitor({ available: true });
    const config: AppConfig = { ...baseConfig, providers: ['whisper_streaming'] };
    const result = await computeProviderAvailability(config, { monitor });
    expect(result[0].available).toBe(true);
    expect(result[0].supportsStreaming).toBe(true);
    expect(result[0].supportsBatch).toBe(true);
    expect(result[0].reason).toBeUndefined();
    expect(monitor.triggerCheck).toHaveBeenCalled();
    expect(monitor.updateRefreshMs).toHaveBeenCalledWith(5000);
  });

  it('exposes the monitor reason when unhealthy', async () => {
    const monitor = createMonitor({ available: false, reason: 'ready check failed' });
    const config: AppConfig = { ...baseConfig, providers: ['whisper_streaming'] };
    const result = await computeProviderAvailability(config, { monitor });
    expect(result[0].available).toBe(false);
    expect(result[0].reason).toBe('ready check failed');
  });
});

describe('computeProviderAvailability feature flags', () => {
  it('reports dictionary/句読点 support per provider', async () => {
    const config: AppConfig = { ...baseConfig, providers: ['deepgram', 'elevenlabs'] };
    const result = await computeProviderAvailability(config);
    const deepgram = result.find((p) => p.id === 'deepgram');
    const elevenlabs = result.find((p) => p.id === 'elevenlabs');
    expect(deepgram).toBeDefined();
    expect(deepgram?.supportsDictionaryPhrases).toBe(true);
    expect(deepgram?.supportsPunctuationPolicy).toBe(true);
    expect(elevenlabs).toBeDefined();
    expect(elevenlabs?.supportsDictionaryPhrases).toBe(false);
    expect(elevenlabs?.supportsPunctuationPolicy).toBe(false);
    expect(elevenlabs?.supportsContextPhrases).toBe(false);
  });

  it('marks openai unavailable when API key is missing', async () => {
    const config: AppConfig = { ...baseConfig, providers: ['openai'] };
    delete process.env.OPENAI_API_KEY;
    const result = await computeProviderAvailability(config);
    const openai = result.find((p) => p.id === 'openai');
    expect(openai).toBeDefined();
    expect(openai?.available).toBe(false);
    expect(openai?.reason).toContain('OPENAI_API_KEY');
  });

  it('reports openai capability flags consistent with implementation', async () => {
    const config: AppConfig = { ...baseConfig, providers: ['openai'] };
    process.env.OPENAI_API_KEY = 'sk-test';
    const result = await computeProviderAvailability(config);
    const openai = result.find((p) => p.id === 'openai');
    expect(openai).toBeDefined();
    expect(openai?.available).toBe(true);
    expect(openai?.supportsDictionaryPhrases).toBe(true);
    expect(openai?.supportsContextPhrases).toBe(true);
    expect(openai?.supportsPunctuationPolicy).toBe(false);
    expect(openai?.supportsDiarization).toBe(false);
  });
});
