import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { ensureNormalizedAudio } from './audioIngress.js';

const { normalizeToPcmWavMock } = vi.hoisted(() => ({
  normalizeToPcmWavMock: vi.fn(),
}));

vi.mock('./audioNormalizer.js', () => ({
  normalizeToPcmWav: normalizeToPcmWavMock,
  AudioDecodeError: class extends Error {},
}));

function createPcmWav(sampleRate: number, channels: number, frameCount = 32): Buffer {
  const bitsPerSample = 16;
  const bytesPerSample = bitsPerSample / 8;
  const blockAlign = channels * bytesPerSample;
  const byteRate = sampleRate * blockAlign;
  const dataSize = frameCount * blockAlign;
  const riffSize = 36 + dataSize;
  const buffer = Buffer.alloc(44 + dataSize);

  buffer.write('RIFF', 0, 'ascii');
  buffer.writeUInt32LE(riffSize, 4);
  buffer.write('WAVE', 8, 'ascii');
  buffer.write('fmt ', 12, 'ascii');
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20); // PCM
  buffer.writeUInt16LE(channels, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(byteRate, 28);
  buffer.writeUInt16LE(blockAlign, 32);
  buffer.writeUInt16LE(bitsPerSample, 34);
  buffer.write('data', 36, 'ascii');
  buffer.writeUInt32LE(dataSize, 40);

  return buffer;
}

const baseConfig = {
  audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
  ingressNormalize: { enabled: false, targetSampleRate: 16000, targetChannels: 1 },
  normalization: {},
  storage: { driver: 'jsonl', path: './runs', retentionDays: 30, maxRows: 100000 },
  providers: ['mock'],
} as const;

describe('ensureNormalizedAudio', () => {
  beforeEach(() => {
    normalizeToPcmWavMock.mockReset();
  });

  it('normalizes to target sample rate/channels even when ingressNormalize.enabled=false', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'audio-ingress-'));
    const source = path.join(dir, 'stereo-44k.wav');
    await writeFile(source, createPcmWav(44100, 2));

    normalizeToPcmWavMock.mockResolvedValue({
      normalizedPath: source,
      durationSec: 1,
      degraded: false,
    });

    const normalized = await ensureNormalizedAudio(source, {
      config: baseConfig as any,
      allowCache: false,
    });

    expect(normalizeToPcmWavMock).toHaveBeenCalledTimes(1);
    expect(normalizeToPcmWavMock).toHaveBeenCalledWith(
      source,
      expect.objectContaining({ targetSampleRate: 16000, targetChannels: 1 })
    );
    await normalized.release();
    await rm(dir, { recursive: true, force: true });
  });

  it('skips conversion when input already matches target spec', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'audio-ingress-'));
    const source = path.join(dir, 'mono-16k.wav');
    await writeFile(source, createPcmWav(16000, 1));

    const normalized = await ensureNormalizedAudio(source, {
      config: baseConfig as any,
      allowCache: false,
    });

    expect(normalizeToPcmWavMock).not.toHaveBeenCalled();
    expect(normalized.normalizedPath).toBe(source);
    await normalized.release();
    await rm(dir, { recursive: true, force: true });
  });
});
