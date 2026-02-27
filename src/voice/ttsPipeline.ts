import type { ProviderId } from '../types.js';
import { streamTtsPcm, ElevenLabsTtsHttpError } from './elevenlabsTts.js';
import { OpenAiTtsHttpError, streamOpenAiTtsPcm } from './openaiTts.js';

export type VoiceTtsStrategy = 'stream' | 'segment-buffered';

export type VoiceTtsConfig = {
  strategy?: VoiceTtsStrategy;
  segmentMinChars?: number;
  segmentMaxChars?: number;
  retryMax?: number;
  retryBaseMs?: number;
};

export type TtsStreamStats = {
  strategy: VoiceTtsStrategy;
  segments: number;
  retries: number;
};

type SentenceSegment = { segment: string };
type SegmenterLike = { segment: (input: string) => Iterable<SentenceSegment> };
type SegmenterCtor = new (locale?: string, options?: { granularity?: 'sentence' | 'word' | 'grapheme' }) => SegmenterLike;

const DEFAULT_TTS_CONFIG: Required<VoiceTtsConfig> = {
  strategy: 'segment-buffered',
  segmentMinChars: 40,
  segmentMaxChars: 140,
  retryMax: 2,
  retryBaseMs: 250,
};

const RETRYABLE_ERROR_CODES = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'EAI_AGAIN',
  'ENOTFOUND',
  'ECONNREFUSED',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
]);

const clampInt = (value: number, min: number, max: number): number => Math.min(max, Math.max(min, Math.round(value)));

const normalizeConfig = (config?: VoiceTtsConfig): Required<VoiceTtsConfig> => {
  const segmentMinChars = config?.segmentMinChars;
  const segmentMaxChars = config?.segmentMaxChars;
  const retryMax = config?.retryMax;
  const retryBaseMs = config?.retryBaseMs;
  const resolved: Required<VoiceTtsConfig> = {
    strategy: config?.strategy ?? DEFAULT_TTS_CONFIG.strategy,
    segmentMinChars: Number.isFinite(segmentMinChars ?? NaN)
      ? clampInt(segmentMinChars as number, 10, 400)
      : DEFAULT_TTS_CONFIG.segmentMinChars,
    segmentMaxChars: Number.isFinite(segmentMaxChars ?? NaN)
      ? clampInt(segmentMaxChars as number, 20, 800)
      : DEFAULT_TTS_CONFIG.segmentMaxChars,
    retryMax: Number.isFinite(retryMax ?? NaN) ? clampInt(retryMax as number, 0, 5) : DEFAULT_TTS_CONFIG.retryMax,
    retryBaseMs: Number.isFinite(retryBaseMs ?? NaN)
      ? clampInt(retryBaseMs as number, 50, 5000)
      : DEFAULT_TTS_CONFIG.retryBaseMs,
  };
  if (resolved.segmentMaxChars < resolved.segmentMinChars) {
    resolved.segmentMaxChars = resolved.segmentMinChars;
  }
  return resolved;
};

const needsSpace = (a: string, b: string): boolean =>
  /[a-z0-9]$/i.test(a) && /^[a-z0-9]/i.test(b);

const concatSegments = (a: string, b: string): string => {
  if (!a) return b;
  if (!b) return a;
  return needsSpace(a, b) ? `${a} ${b}` : `${a}${b}`;
};

const fallbackSentenceSplit = (text: string): string[] => {
  const matches = text.match(/[^。！？.!?]+[。！？.!?]?/g);
  if (!matches || matches.length === 0) return [text];
  return matches;
};

const splitIntoUnits = (text: string, lang?: string): string[] => {
  const trimmed = text.trim();
  if (!trimmed) return [];
  try {
    const Segmenter = (Intl as { Segmenter?: SegmenterCtor }).Segmenter;
    if (Segmenter) {
      const segmenter = new Segmenter(lang, { granularity: 'sentence' });
      const units = Array.from(segmenter.segment(trimmed))
        .map((seg) => seg.segment.trim())
        .filter(Boolean);
      if (units.length > 0) return units;
    }
  } catch {
    // ignore segmenter failures and fallback
  }
  return fallbackSentenceSplit(trimmed).map((unit) => unit.trim()).filter(Boolean);
};

const splitLongText = (text: string, maxChars: number): string[] => {
  const chunks: string[] = [];
  let remaining = text.trim();
  while (remaining.length > maxChars) {
    const slice = remaining.slice(0, maxChars);
    const breakpoints = ['。', '！', '？', '!', '?', '.', '、', ',', ';', ':', ' '];
    let cut = -1;
    for (const token of breakpoints) {
      const idx = slice.lastIndexOf(token);
      if (idx > cut) cut = idx;
    }
    const cutIndex = cut >= Math.floor(maxChars * 0.6) ? cut + 1 : maxChars;
    const head = remaining.slice(0, cutIndex).trim();
    if (head) chunks.push(head);
    remaining = remaining.slice(cutIndex).trim();
  }
  if (remaining) chunks.push(remaining);
  return chunks;
};

const mergeSegments = (segments: string[], minChars: number, maxChars: number): string[] => {
  if (segments.length <= 1) return segments;
  const merged: string[] = [];
  for (const segment of segments) {
    if (!segment) continue;
    if (merged.length === 0) {
      merged.push(segment);
      continue;
    }
    const prev = merged[merged.length - 1];
    if (prev.length < minChars) {
      const combined = concatSegments(prev, segment);
      if (combined.length <= maxChars) {
        merged[merged.length - 1] = combined;
        continue;
      }
    }
    merged.push(segment);
  }
  return merged;
};

const buildSegments = (text: string, lang: string | undefined, minChars: number, maxChars: number): string[] => {
  const units = splitIntoUnits(text, lang);
  if (units.length === 0) return [];
  const segments: string[] = [];
  let current = '';
  for (const unit of units) {
    if (!unit) continue;
    const candidate = concatSegments(current, unit);
    if (candidate.length <= maxChars) {
      current = candidate;
      continue;
    }
    if (current) {
      segments.push(current.trim());
      current = '';
    }
    if (unit.length > maxChars) {
      segments.push(...splitLongText(unit, maxChars));
      continue;
    }
    current = unit;
  }
  if (current.trim()) {
    segments.push(current.trim());
  }
  return mergeSegments(segments, minChars, maxChars);
};

const toErrorCode = (err: unknown): string | null => {
  if (!err || typeof err !== 'object') return null;
  const code = (err as { code?: unknown }).code;
  if (typeof code === 'string') return code;
  const cause = (err as { cause?: unknown }).cause;
  if (cause && typeof cause === 'object') {
    const causeCode = (cause as { code?: unknown }).code;
    if (typeof causeCode === 'string') return causeCode;
  }
  return null;
};

const isRetryableTtsError = (err: unknown): boolean => {
  if (!err || !(err instanceof Error)) return false;
  if (err.name === 'AbortError') return false;
  if (err instanceof OpenAiTtsHttpError || err instanceof ElevenLabsTtsHttpError) {
    return err.status === 408 || err.status === 429 || err.status >= 500;
  }
  const code = toErrorCode(err);
  if (code && RETRYABLE_ERROR_CODES.has(code)) return true;
  const message = err.message.toLowerCase();
  if (message.includes('terminated')) return true;
  if (message.includes('fetch failed')) return true;
  if (message.includes('socket') && message.includes('error')) return true;
  if (message.includes('timeout')) return true;
  return false;
};

const sleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, ms);
    if (signal) {
      signal.addEventListener(
        'abort',
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true }
      );
    }
  });

const collectSegment = async (
  stream: AsyncIterable<Buffer>,
  signal?: AbortSignal
): Promise<{ chunks: Buffer[]; aborted: boolean }> => {
  const chunks: Buffer[] = [];
  try {
    for await (const chunk of stream) {
      if (signal?.aborted) return { chunks: [], aborted: true };
      chunks.push(chunk);
    }
  } catch (err) {
    if (signal?.aborted) return { chunks: [], aborted: true };
    throw err;
  }
  if (signal?.aborted) return { chunks: [], aborted: true };
  return { chunks, aborted: false };
};

const createProviderStream = (
  provider: ProviderId,
  text: string,
  opts: { signal?: AbortSignal; sampleRate: number; lang?: string }
): AsyncGenerator<Buffer> => {
  if (provider === 'openai') {
    return streamOpenAiTtsPcm(text, { signal: opts.signal, sampleRate: opts.sampleRate });
  }
  if (provider === 'elevenlabs') {
    return streamTtsPcm(text, { signal: opts.signal, sampleRate: opts.sampleRate, lang: opts.lang });
  }
  throw new Error(`unsupported TTS provider: ${provider}`);
};

const backoffDelay = (baseMs: number, attempt: number): number => {
  const jitter = Math.round(Math.random() * baseMs);
  return Math.round(baseMs * Math.pow(2, attempt)) + jitter;
};

export function createTtsStream(options: {
  provider: ProviderId;
  text: string;
  lang?: string;
  sampleRate: number;
  signal?: AbortSignal;
  config?: VoiceTtsConfig;
}): { stream: AsyncGenerator<Buffer>; stats: TtsStreamStats } {
  const resolved = normalizeConfig(options.config);
  const stats: TtsStreamStats = {
    strategy: resolved.strategy,
    segments: 0,
    retries: 0,
  };
  const normalizedText = options.text.trim();
  if (!normalizedText) {
    return {
      stream: (async function* empty() {})(),
      stats,
    };
  }

  const segments =
    resolved.strategy === 'segment-buffered'
      ? buildSegments(normalizedText, options.lang, resolved.segmentMinChars, resolved.segmentMaxChars)
      : [normalizedText];
  stats.segments = segments.length;

  const stream = (async function* () {
    if (segments.length === 0) return;
    if (resolved.strategy === 'stream') {
      yield* createProviderStream(options.provider, normalizedText, {
        signal: options.signal,
        sampleRate: options.sampleRate,
        lang: options.lang,
      });
      return;
    }

    for (const segment of segments) {
      if (options.signal?.aborted) return;
      let attempt = 0;
      while (true) {
        try {
          const providerStream = createProviderStream(options.provider, segment, {
            signal: options.signal,
            sampleRate: options.sampleRate,
            lang: options.lang,
          });
          const collected = await collectSegment(providerStream, options.signal);
          if (collected.aborted) return;
          for (const chunk of collected.chunks) {
            yield chunk;
          }
          break;
        } catch (err) {
          if (options.signal?.aborted) return;
          if (!isRetryableTtsError(err) || attempt >= resolved.retryMax) {
            throw err;
          }
          stats.retries += 1;
          const delayMs = backoffDelay(resolved.retryBaseMs, attempt);
          await sleep(delayMs, options.signal);
          attempt += 1;
        }
      }
    }
  })();

  return { stream, stats };
}
