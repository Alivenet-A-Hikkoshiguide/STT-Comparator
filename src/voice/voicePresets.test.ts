import { afterEach, describe, expect, it, vi } from 'vitest';
import type { AppConfig } from '../config.js';

const envSnapshot = {
  OPENAI_API_KEY: process.env.OPENAI_API_KEY,
  ELEVENLABS_API_KEY: process.env.ELEVENLABS_API_KEY,
  ELEVENLABS_TTS_VOICE_ID: process.env.ELEVENLABS_TTS_VOICE_ID,
  DEEPGRAM_API_KEY: process.env.DEEPGRAM_API_KEY,
  VOICE_PRESET_ID: process.env.VOICE_PRESET_ID,
};

afterEach(() => {
  process.env.OPENAI_API_KEY = envSnapshot.OPENAI_API_KEY;
  process.env.ELEVENLABS_API_KEY = envSnapshot.ELEVENLABS_API_KEY;
  process.env.ELEVENLABS_TTS_VOICE_ID = envSnapshot.ELEVENLABS_TTS_VOICE_ID;
  process.env.DEEPGRAM_API_KEY = envSnapshot.DEEPGRAM_API_KEY;
  process.env.VOICE_PRESET_ID = envSnapshot.VOICE_PRESET_ID;
  vi.resetModules();
});

function baseConfig(extra?: Partial<AppConfig>): AppConfig {
  return {
    audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
    normalization: {},
    storage: { driver: 'jsonl', path: './runs/latest', retentionDays: 30, maxRows: 100000 },
    providers: ['openai'],
    jobs: { maxParallel: 4, retentionMs: 10 * 60 * 1000, retry: { maxAttempts: 3, baseDelayMs: 1000, maxDelayMs: 10_000 } },
    ws: {},
    providerHealth: {},
    providerLimits: {},
    ...extra,
  };
}

describe('voicePresets', () => {
  it('returns built-in presets when config.voice.presets is unset', async () => {
    process.env.OPENAI_API_KEY = 'test-openai';
    delete process.env.ELEVENLABS_API_KEY;
    delete process.env.ELEVENLABS_TTS_VOICE_ID;
    delete process.env.DEEPGRAM_API_KEY;
    delete process.env.VOICE_PRESET_ID;
    vi.resetModules();

    const { getVoicePresetCatalog } = await import('./voicePresets.js');
    const catalog = getVoicePresetCatalog(baseConfig());

    expect(catalog.presets.map((p) => p.id)).toEqual(['elevenlabs', 'openai_realtime', 'openai', 'deepgram']);
    const openai = catalog.presets.find((p) => p.id === 'openai');
    expect(openai?.available).toBe(true);
  });

  it('resolves the requested preset id', async () => {
    process.env.OPENAI_API_KEY = 'test-openai';
    vi.resetModules();

    const { resolveVoicePreset } = await import('./voicePresets.js');
    const preset = resolveVoicePreset(baseConfig(), 'openai');
    expect(preset.id).toBe('openai');
  });

  it('uses config.voice presets and defaultPresetId', async () => {
    process.env.OPENAI_API_KEY = 'test-openai';
    vi.resetModules();

    const { getVoicePresetCatalog } = await import('./voicePresets.js');

    const cfg = baseConfig({
      voice: {
        defaultPresetId: 'custom',
        presets: [{ id: 'custom', label: 'Custom', sttProvider: 'openai', ttsProvider: 'openai' }],
      },
    });

    const catalog = getVoicePresetCatalog(cfg);
    expect(catalog.defaultPresetId).toBe('custom');
    expect(catalog.presets.map((p) => p.id)).toEqual(['custom']);
  });
});
