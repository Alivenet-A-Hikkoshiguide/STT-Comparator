import { PassThrough, Readable } from 'node:stream';
import { createPcmResampler } from '../utils/ffmpeg.js';
import { withTimeoutSignal } from '../utils/abort.js';

const OPENAI_TTS_URL = 'https://api.openai.com/v1/audio/speech';
const OPENAI_TTS_INPUT_SAMPLE_RATE = 24_000;

export class OpenAiTtsHttpError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'OpenAiTtsHttpError';
    this.status = status;
  }
}

function requireApiKey(): string {
  const key = process.env.OPENAI_API_KEY;
  if (!key) {
    throw new Error('OpenAI API key is required. Set OPENAI_API_KEY in .env');
  }
  return key;
}

function getModel(): string {
  return process.env.OPENAI_TTS_MODEL ?? 'gpt-4o-mini-tts';
}

function getVoice(): string {
  return process.env.OPENAI_TTS_VOICE ?? 'alloy';
}

function getFrameMs(): number {
  const raw = Number(process.env.OPENAI_TTS_FRAME_MS);
  if (Number.isFinite(raw) && raw > 0) return Math.min(500, Math.max(10, Math.round(raw)));
  return 40;
}

function getTimeoutMs(): number {
  const raw = Number(process.env.OPENAI_TTS_TIMEOUT_MS);
  if (Number.isFinite(raw) && raw > 0) return Math.min(5 * 60_000, Math.round(raw));
  return 60_000;
}

function normalizeChunk(chunk: unknown): Buffer {
  if (Buffer.isBuffer(chunk)) return chunk;
  if (chunk instanceof ArrayBuffer) return Buffer.from(chunk);
  if (ArrayBuffer.isView(chunk)) {
    return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }
  if (typeof chunk === 'string') return Buffer.from(chunk);
  return Buffer.from(chunk as Uint8Array);
}

async function createTtsResponse(
  text: string,
  apiKey: string,
  signal?: AbortSignal
): Promise<Response> {
  const payload = {
    model: getModel(),
    voice: getVoice(),
    input: text,
    // Lowest-latency output: raw PCM16 @ 24kHz (per OpenAI docs).
    response_format: 'pcm',
  };

  return await fetch(OPENAI_TTS_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
    signal,
  });
}

export async function* streamOpenAiTtsPcm(
  text: string,
  options: { signal?: AbortSignal; sampleRate: number }
): AsyncGenerator<Buffer> {
  const apiKey = requireApiKey();
  const frameMs = getFrameMs();
  const timeoutMs = getTimeoutMs();

  const { signal, didTimeout, cleanup } = withTimeoutSignal({
    signal: options.signal,
    timeoutMs,
  });

  try {
    const res = await createTtsResponse(text, apiKey, signal);
    if (!res.ok) {
      const detail = await res.text().catch(() => '');
      throw new OpenAiTtsHttpError(res.status, `OpenAI TTS error (${res.status}): ${detail.slice(0, 300)}`);
    }
    if (!res.body) {
      throw new Error('OpenAI TTS response missing body');
    }

    const bytesPerSample = 2; // mono 16-bit
    const frameSamples = Math.max(1, Math.round((options.sampleRate * frameMs) / 1000));
    const frameBytes = frameSamples * bytesPerSample;

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

    const inputStream = Readable.fromWeb(res.body as any);

    if (options.sampleRate === OPENAI_TTS_INPUT_SAMPLE_RATE) {
      try {
        for await (const frame of framePcm(inputStream)) {
          yield frame;
        }
      } finally {
        inputStream.destroy();
      }
      return;
    }

    const resampler = createPcmResampler({
      inputSampleRate: OPENAI_TTS_INPUT_SAMPLE_RATE,
      outputSampleRate: options.sampleRate,
      channels: 1,
    });

    const out = new PassThrough();
    let fatal: Error | null = null;

    const closePromise = new Promise<void>((resolve, reject) => {
      resampler.onError((err) => {
        fatal = err;
        out.destroy(err);
        reject(err);
      });
      resampler.onClose((code) => {
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

    resampler.onChunk((chunk) => {
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
            const durationMs = (alignedLength / bytesPerSample / OPENAI_TTS_INPUT_SAMPLE_RATE) * 1000;
            await resampler.input(slice, { captureTs: Date.now(), durationMs, seq });
            seq += 1;
          }
          carry = combined.subarray(alignedLength);
        }
      } finally {
        resampler.end();
        inputStream.destroy();
      }
    })();

    try {
      for await (const frame of framePcm(out)) {
        yield frame;
      }
    } finally {
      out.end();
      resampler.end();
      inputStream.destroy();
      await pumpPromise.catch(() => undefined);
      await closePromise.catch(() => undefined);
    }

    if (!signal?.aborted && fatal) {
      throw fatal;
    }

    if (didTimeout()) {
      throw new Error(`OpenAI TTS request timed out after ${timeoutMs}ms`);
    }
  } catch (err) {
    if (didTimeout()) {
      throw new Error(`OpenAI TTS request timed out after ${timeoutMs}ms`);
    }
    throw err instanceof Error ? err : new Error('OpenAI TTS request failed');
  } finally {
    cleanup();
  }
}
