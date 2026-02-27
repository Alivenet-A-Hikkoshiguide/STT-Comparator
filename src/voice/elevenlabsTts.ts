import { PassThrough, Readable } from 'node:stream';
import { createPcmResampler, spawnPcmTranscoder } from '../utils/ffmpeg.js';
import { withTimeoutSignal } from '../utils/abort.js';
import { logger } from '../logger.js';

const ELEVENLABS_TTS_URL_BASE = 'https://api.elevenlabs.io/v1/text-to-speech';
const DEFAULT_FALLBACK_OUTPUT_FORMAT = 'mp3_22050_32';
const DEFAULT_PCM_SAMPLE_RATES = new Set([16_000, 22_050, 24_000, 44_100]);
const DEFAULT_JA_MODEL_ID = 'eleven_multilingual_v2';

export class ElevenLabsTtsHttpError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ElevenLabsTtsHttpError';
    this.status = status;
  }
}

function requireApiKey(): string {
  const key = process.env.ELEVENLABS_API_KEY;
  if (!key) {
    throw new Error('ElevenLabs API key is required. Set ELEVENLABS_API_KEY in .env');
  }
  return key;
}

function requireVoiceId(): string {
  const voiceId = process.env.ELEVENLABS_TTS_VOICE_ID;
  if (!voiceId) {
    throw new Error('ElevenLabs TTS voice id is required. Set ELEVENLABS_TTS_VOICE_ID in .env');
  }
  return voiceId;
}

function getFrameMs(): number {
  const raw = Number(process.env.ELEVENLABS_TTS_FRAME_MS);
  if (Number.isFinite(raw) && raw > 0) return Math.min(500, Math.max(10, Math.round(raw)));
  return 40;
}

function getTimeoutMs(): number {
  const raw = Number(process.env.ELEVENLABS_TTS_TIMEOUT_MS);
  if (Number.isFinite(raw) && raw > 0) return Math.min(5 * 60_000, Math.round(raw));
  return 60_000;
}

function normalizeOptionalEnv(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function isJapaneseLocale(lang: string | undefined): boolean {
  if (!lang) return false;
  return lang.toLowerCase().startsWith('ja');
}

function pickDefaultOutputFormat(sampleRate: number): string {
  if (DEFAULT_PCM_SAMPLE_RATES.has(sampleRate)) {
    return `pcm_${sampleRate}`;
  }
  return DEFAULT_FALLBACK_OUTPUT_FORMAT;
}

function parsePcmOutputSampleRate(outputFormat: string): number | null {
  if (!outputFormat.startsWith('pcm_')) return null;
  const raw = Number(outputFormat.slice('pcm_'.length));
  if (!Number.isFinite(raw) || raw <= 0) return null;
  return Math.round(raw);
}

async function createTtsResponse(
  url: URL,
  payload: Record<string, unknown>,
  apiKey: string,
  signal?: AbortSignal
): Promise<Response> {
  return await fetch(url.toString(), {
    method: 'POST',
    headers: {
      'xi-api-key': apiKey,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
    signal,
  });
}

export async function* streamTtsPcm(
  text: string,
  options: { signal?: AbortSignal; sampleRate: number; lang?: string }
): AsyncGenerator<Buffer> {
  const apiKey = requireApiKey();
  const voiceId = requireVoiceId();
  const modelIdEnv = normalizeOptionalEnv(process.env.ELEVENLABS_TTS_MODEL_ID);
  const outputFormatEnv = normalizeOptionalEnv(process.env.ELEVENLABS_TTS_OUTPUT_FORMAT);
  const optimizeStreamingLatency = normalizeOptionalEnv(process.env.ELEVENLABS_TTS_OPTIMIZE_STREAMING_LATENCY);
  const frameMs = getFrameMs();
  const timeoutMs = getTimeoutMs();

  const autoModelId = !modelIdEnv && isJapaneseLocale(options.lang) ? DEFAULT_JA_MODEL_ID : undefined;
  const payloadCandidates: Array<{ payload: Record<string, unknown>; modelId: string | null; source: 'env' | 'auto' | 'none' }> =
    modelIdEnv
      ? [
          { payload: { text, model_id: modelIdEnv }, modelId: modelIdEnv, source: 'env' },
          { payload: { text }, modelId: null, source: 'none' },
        ]
      : autoModelId
        ? [
            { payload: { text, model_id: autoModelId }, modelId: autoModelId, source: 'auto' },
            { payload: { text }, modelId: null, source: 'none' },
          ]
        : [{ payload: { text }, modelId: null, source: 'none' }];

  const { signal, didTimeout, cleanup } = withTimeoutSignal({
    signal: options.signal,
    timeoutMs,
  });

  try {
    const requestedOutputFormat = outputFormatEnv ?? pickDefaultOutputFormat(options.sampleRate);
    const shouldFallbackFormat = !outputFormatEnv && requestedOutputFormat.startsWith('pcm_');

    const tryRequest = async (payload: Record<string, unknown>) => {
      const url = new URL(`${ELEVENLABS_TTS_URL_BASE}/${voiceId}/stream`);
      url.searchParams.set('output_format', requestedOutputFormat);
      if (optimizeStreamingLatency) {
        url.searchParams.set('optimize_streaming_latency', optimizeStreamingLatency);
      }

      let effectiveOutputFormat = requestedOutputFormat;
      let usedFallbackFormat = false;
      let res = await createTtsResponse(url, payload, apiKey, signal);
      if (!res.ok && shouldFallbackFormat && res.status === 400) {
        await res.text().catch(() => '');
        const fallbackUrl = new URL(url);
        fallbackUrl.searchParams.set('output_format', DEFAULT_FALLBACK_OUTPUT_FORMAT);
        res = await createTtsResponse(fallbackUrl, payload, apiKey, signal);
        effectiveOutputFormat = DEFAULT_FALLBACK_OUTPUT_FORMAT;
        usedFallbackFormat = true;
      }
      if (!res.ok) {
        const detail = await res.text().catch(() => '');
        throw new ElevenLabsTtsHttpError(res.status, `ElevenLabs TTS error (${res.status}): ${detail.slice(0, 300)}`);
      }
      if (!res.body) {
        throw new Error('ElevenLabs TTS response missing body');
      }
      return { res, effectiveOutputFormat, usedFallbackFormat };
    };

    let res: Response | null = null;
    let effectiveOutputFormat = requestedOutputFormat;
    let usedFallbackFormat = false;
    let selectedModelId: string | null = null;
    let selectedModelSource: 'env' | 'auto' | 'none' = 'none';

    for (const candidate of payloadCandidates) {
      try {
        const attempted = await tryRequest(candidate.payload);
        res = attempted.res;
        effectiveOutputFormat = attempted.effectiveOutputFormat;
        usedFallbackFormat = attempted.usedFallbackFormat;
        selectedModelId = candidate.modelId;
        selectedModelSource = candidate.source;
        break;
      } catch (err) {
        const httpStatus = err instanceof ElevenLabsTtsHttpError ? err.status : null;
        const shouldRetryWithoutModel =
          candidate.modelId !== null && httpStatus !== null && (httpStatus === 400 || httpStatus === 422);
        if (shouldRetryWithoutModel) {
          logger.info({
            event: 'elevenlabs_tts_model_fallback',
            fromModelId: candidate.modelId,
            fromModelSource: candidate.source,
            status: httpStatus,
          });
          continue;
        }
        throw err;
      }
    }

    if (!res) {
      throw new Error('ElevenLabs TTS request failed');
    }

    const bytesPerSample = 2;
    const frameSamples = Math.max(1, Math.round((options.sampleRate * frameMs) / 1000));
    const frameBytes = frameSamples * bytesPerSample;

    const inputStream = Readable.fromWeb(res.body as any);

    const normalizeChunk = (chunk: unknown): Buffer => {
      if (Buffer.isBuffer(chunk)) return chunk;
      if (chunk instanceof ArrayBuffer) return Buffer.from(chunk);
      if (ArrayBuffer.isView(chunk)) {
        return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
      }
      if (typeof chunk === 'string') return Buffer.from(chunk);
      return Buffer.from(chunk as Uint8Array);
    };

    const inputPcmSampleRate = parsePcmOutputSampleRate(effectiveOutputFormat);
    const treatAsPcm = inputPcmSampleRate !== null;

    const logPayload = {
      event: 'elevenlabs_tts_stream_config',
      requestedOutputFormat,
      effectiveOutputFormat,
      usedFallbackFormat,
      treatAsPcm,
      inputPcmSampleRate,
      outputSampleRate: options.sampleRate,
      frameMs,
      optimizeStreamingLatency,
      modelId: selectedModelId ?? undefined,
      modelIdSource: selectedModelSource,
    } as const;

    if (usedFallbackFormat || !treatAsPcm || inputPcmSampleRate !== options.sampleRate || selectedModelSource === 'auto') {
      logger.info(logPayload);
    } else {
      logger.debug(logPayload);
    }

    const framePcm = async function* (pcmSource: AsyncIterable<unknown>): AsyncGenerator<Buffer> {
      let carry: Buffer<ArrayBufferLike> = Buffer.alloc(0);
      for await (const rawChunk of pcmSource) {
        if (signal?.aborted) break;
        const chunk = normalizeChunk(rawChunk);
        const combined = (carry.length > 0 ? Buffer.concat([carry, chunk]) : chunk) as Buffer<ArrayBufferLike>;
        const alignedLength = combined.length - (combined.length % bytesPerSample);
        const aligned = combined.subarray(0, alignedLength);
        let offset = 0;
        while (alignedLength - offset >= frameBytes) {
          yield aligned.subarray(offset, offset + frameBytes);
          offset += frameBytes;
        }
        carry = combined.subarray(offset);
      }
      if (!signal?.aborted && carry.length > 0) {
        const alignedLength = carry.length - (carry.length % bytesPerSample);
        if (alignedLength > 0) {
          yield carry.subarray(0, alignedLength);
        }
      }
    };

    if (treatAsPcm && inputPcmSampleRate === options.sampleRate) {
      try {
        for await (const frame of framePcm(inputStream)) {
          yield frame;
        }
      } finally {
        inputStream.destroy();
      }
    } else if (treatAsPcm && inputPcmSampleRate !== null) {
      const spawned = createPcmResampler({
        inputSampleRate: inputPcmSampleRate,
        outputSampleRate: options.sampleRate,
        channels: 1,
      });
      const out = new PassThrough();
      let fatal: Error | null = null;
      const closePromise = new Promise<void>((resolve, reject) => {
        spawned.onError((err) => {
          fatal = err;
          out.destroy(err);
          reject(err);
        });
        spawned.onClose((code) => {
          if (typeof code === 'number' && code !== 0) {
            const err = new Error(`ffmpeg exited with code ${code}`);
            fatal = err;
            out.destroy(err);
            reject(err);
            return;
          }
          out.end();
          resolve();
        });
      });
      spawned.onChunk((chunk) => {
        out.write(chunk);
      });

      const pumpPromise = (async () => {
        let seq = 0;
        let carry: Buffer<ArrayBufferLike> = Buffer.alloc(0);
        try {
          for await (const raw of inputStream) {
            if (signal?.aborted) break;
            const buf = normalizeChunk(raw);
            const combined = (carry.length > 0 ? Buffer.concat([carry, buf]) : buf) as Buffer<ArrayBufferLike>;
            const alignedLength = combined.length - (combined.length % bytesPerSample);
            if (alignedLength > 0) {
              const slice = combined.subarray(0, alignedLength);
              const durationMs = (alignedLength / bytesPerSample / inputPcmSampleRate) * 1000;
              await spawned.input(slice, { captureTs: Date.now(), durationMs, seq });
              seq += 1;
            }
            carry = combined.subarray(alignedLength);
          }
        } finally {
          spawned.end();
          inputStream.destroy();
        }
      })();

      try {
        for await (const frame of framePcm(out)) {
          yield frame;
        }
      } finally {
        out.end();
        spawned.end();
        inputStream.destroy();
        await pumpPromise.catch(() => undefined);
        await closePromise.catch(() => undefined);
      }

      if (!signal?.aborted && fatal) {
        throw fatal;
      }
    } else {
      const transcoder = spawnPcmTranscoder({
        targetChannels: 1,
        targetSampleRate: options.sampleRate,
        chunkMs: 250,
      });
      let fatal: Error | null = null;
      const closePromise = new Promise<void>((resolve, reject) => {
        transcoder.onError((err) => {
          fatal = err;
          reject(err);
        });
        transcoder.onClose((code) => {
          if (typeof code === 'number' && code !== 0) {
            const err = new Error(`ffmpeg exited with code ${code}`);
            fatal = err;
            reject(err);
            return;
          }
          resolve();
        });
      });
      const pumpPromise = (async () => {
        try {
          for await (const chunk of inputStream) {
            if (signal?.aborted) break;
            const buf = normalizeChunk(chunk);
            await transcoder.input(buf);
          }
        } finally {
          transcoder.end();
          inputStream.destroy();
        }
      })();

      try {
        for await (const frame of framePcm(transcoder.stream)) {
          yield frame;
        }
      } finally {
        transcoder.end();
        inputStream.destroy();
        await pumpPromise.catch(() => undefined);
        await closePromise.catch(() => undefined);
      }

      if (!signal?.aborted && fatal) {
        throw fatal;
      }
    }

    if (didTimeout()) {
      throw new Error(`ElevenLabs TTS request timed out after ${timeoutMs}ms`);
    }
  } catch (err) {
    if (didTimeout()) {
      throw new Error(`ElevenLabs TTS request timed out after ${timeoutMs}ms`);
    }
    throw err instanceof Error ? err : new Error('ElevenLabs TTS request failed');
  } finally {
    cleanup();
  }
}
