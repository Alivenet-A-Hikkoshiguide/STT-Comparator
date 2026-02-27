import type { Readable } from 'node:stream';
import { WebSocket } from 'ws';
import type { BatchResult, PartialTranscript, StreamingOptions, StreamingSession, TranscriptWord } from '../types.js';
import { BaseAdapter } from './base.js';

const DEEPGRAM_WS = 'wss://api.deepgram.com/v1/listen';
const DEEPGRAM_HTTP = 'https://api.deepgram.com/v1/listen';
const DEFAULT_DEEPGRAM_MODEL = process.env.DEEPGRAM_MODEL ?? 'nova-3';
const DEFAULT_DEEPGRAM_TIER = process.env.DEEPGRAM_TIER;
const ENABLE_SMART_FORMAT = process.env.DEEPGRAM_SMART_FORMAT !== '0';
const IDLE_TIMEOUT_MS = 30_000;
const HARD_TIMEOUT_MS = 5 * 60 * 1000;
const STREAM_IDLE_TIMEOUT_MS = 30_000;
const STREAM_BUFFER_HIGH_WATER_BYTES = 5 * 1024 * 1024;
const DEFAULT_ENDPOINTING_MS = 400;

const SUPPORTED_DEEPGRAM_LANGUAGES = [
  'multi',
  'bg',
  'ca',
  'zh',
  'zh-CN',
  'zh-TW',
  'zh-HK',
  'cs',
  'da',
  'da-DK',
  'nl',
  'en',
  'en-US',
  'en-AU',
  'en-GB',
  'en-NZ',
  'en-IN',
  'et',
  'fi',
  'nl-BE',
  'fr',
  'fr-CA',
  'de',
  'de-CH',
  'el',
  'hi',
  'hu',
  'id',
  'it',
  'ja',
  'ko',
  'ko-KR',
  'lv',
  'lt',
  'ms',
  'no',
  'pl',
  'pt',
  'pt-BR',
  'pt-PT',
  'ro',
  'ru',
  'sk',
  'es',
  'es-419',
  'sv',
  'sv-SE',
  'th',
  'th-TH',
  'tr',
  'uk',
  'vi',
] as const;
const SUPPORTED_DEEPGRAM_LANGUAGE_SET = new Set<string>(SUPPORTED_DEEPGRAM_LANGUAGES);

interface DeepgramWord {
  start?: number;
  end?: number;
  word?: string;
  confidence?: number;
  speaker?: number | string;
}

interface DeepgramAlternative {
  transcript?: string;
  words?: DeepgramWord[];
  duration?: number;
  confidence?: number;
  speaker?: number | string;
}

interface DeepgramStreamingChannel {
  alternatives?: DeepgramAlternative[];
}

interface DeepgramStreamingMessage {
  type?: string;
  channel?: DeepgramStreamingChannel;
  is_final?: boolean;
  isFinal?: boolean;
  message?: string;
}

interface DeepgramBatchMetadata {
  duration?: number;
  processing_ms?: number;
  processing_time?: number;
}

interface DeepgramUtterance {
  transcript?: string;
  words?: DeepgramWord[];
}

interface DeepgramBatchResult {
  channels?: { alternatives?: DeepgramAlternative[] }[];
  alternatives?: DeepgramAlternative[];
  transcript?: string;
  utterances?: DeepgramUtterance[];
}

interface DeepgramBatchResponse {
  results?: DeepgramBatchResult[] | DeepgramBatchResult;
  metadata?: DeepgramBatchMetadata;
  utterances?: DeepgramUtterance[];
}

const mapDeepgramWord = (word: DeepgramWord): TranscriptWord => ({
  startSec: Number(word.start ?? 0),
  endSec: Number(word.end ?? 0),
  text: word.word ?? '',
  confidence: typeof word.confidence === 'number' ? word.confidence : undefined,
});

function requireApiKey(): string {
  const key = process.env.DEEPGRAM_API_KEY;
  if (!key) {
    throw new Error('Deepgram API key is required. Set DEEPGRAM_API_KEY in .env');
  }
  return key;
}

async function collectStream(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  await new Promise<void>((resolve, reject) => {
    stream.on('data', (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    stream.once('end', () => resolve());
    stream.once('error', (err) => reject(err));
  });
  return Buffer.concat(chunks);
}

function normalizeDeepgramLanguage(lang: string): string {
  const trimmed = lang.trim();
  if (!trimmed) {
    throw new Error('Deepgram language parameter is missing');
  }
  if (SUPPORTED_DEEPGRAM_LANGUAGE_SET.has(trimmed)) {
    return trimmed;
  }
  const lower = trimmed.toLowerCase();
  for (const supported of SUPPORTED_DEEPGRAM_LANGUAGES) {
    if (supported.toLowerCase() === lower) {
      return supported;
    }
  }
  const primary = trimmed.split('-')[0];
  for (const supported of SUPPORTED_DEEPGRAM_LANGUAGES) {
    if (supported.toLowerCase() === primary.toLowerCase()) {
      return supported;
    }
  }
  throw new Error(`Deepgram language "${lang}" is not supported`);
}

function joinPhrases(values?: readonly string[]): string | undefined {
  if (!values?.length) return undefined;
  const normalized = values.map((value) => value.trim()).filter((value) => value.length > 0);
  return normalized.length > 0 ? normalized.join(',') : undefined;
}

function appendPhraseParameters(query: URLSearchParams, opts: Pick<StreamingOptions, 'dictionaryPhrases' | 'contextPhrases'>) {
  const keywords = joinPhrases(opts.dictionaryPhrases);
  if (keywords) {
    query.set('keywords', keywords);
  }
  const context = joinPhrases(opts.contextPhrases);
  if (context) {
    query.set('context', context);
  }
}

function appendVadParameters(query: URLSearchParams, enableVad?: boolean) {
  if (enableVad === false) {
    query.set('endpointing', 'false');
    return;
  }
  if (enableVad === true) {
    query.set('endpointing', String(DEFAULT_ENDPOINTING_MS));
    query.set('vad_events', 'true');
  }
}

function normalizeTranscript(value?: string): string | null {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

function normalizeDeepgramResults(
  results?: DeepgramBatchResult[] | DeepgramBatchResult
): DeepgramBatchResult[] {
  if (!results) return [];
  return Array.isArray(results) ? results : [results];
}

function flattenDeepgramAlternatives(result: DeepgramBatchResult): DeepgramAlternative[] {
  const channelAlternatives =
    result.channels?.flatMap((channel) => channel.alternatives ?? []) ?? [];
  const directAlternatives = result.alternatives ?? [];
  return [...channelAlternatives, ...directAlternatives];
}

function collectAlternativeTranscripts(result: DeepgramBatchResult): string[] {
  return flattenDeepgramAlternatives(result)
    .map((alt) => normalizeTranscript(alt.transcript))
    .filter((value): value is string => Boolean(value));
}

function collectUtteranceTranscripts(utterances?: DeepgramUtterance[]): string[] {
  return (utterances ?? [])
    .map((utterance) => normalizeTranscript(utterance.transcript))
    .filter((value): value is string => Boolean(value));
}

function extractDeepgramTranscripts(json: DeepgramBatchResponse): string[] {
  const transcripts: string[] = [];
  for (const result of normalizeDeepgramResults(json.results)) {
    transcripts.push(...collectAlternativeTranscripts(result));
    transcripts.push(...collectUtteranceTranscripts(result.utterances));
    const raw = normalizeTranscript(result.transcript);
    if (raw) {
      transcripts.push(raw);
    }
  }
  transcripts.push(...collectUtteranceTranscripts(json.utterances));
  return transcripts;
}

function findFirstDeepgramAlternative(json: DeepgramBatchResponse): DeepgramAlternative | undefined {
  for (const result of normalizeDeepgramResults(json.results)) {
    const alternatives = flattenDeepgramAlternatives(result);
    if (alternatives.length > 0) {
      return alternatives[0];
    }
  }
  return undefined;
}

export class DeepgramAdapter extends BaseAdapter {
  id = 'deepgram' as const;
  supportsStreaming = true;
  supportsBatch = true;

  async startStreaming(opts: StreamingOptions): Promise<StreamingSession> {
    const apiKey = requireApiKey();
    const language = normalizeDeepgramLanguage(opts.language);
    const query = new URLSearchParams({
      encoding: opts.encoding,
      sample_rate: String(opts.sampleRateHz),
      channels: '1',
      language,
      punctuate: opts.punctuationPolicy === 'none' ? 'false' : 'true',
    });
    query.set('model', DEFAULT_DEEPGRAM_MODEL);
    if (DEFAULT_DEEPGRAM_TIER) {
      query.set('tier', DEFAULT_DEEPGRAM_TIER);
    }
    if (ENABLE_SMART_FORMAT) {
      query.set('smart_format', 'true');
    }
    if (opts.enableDiarization) {
      query.set('diarize', 'true');
    }
    if (opts.enableInterim === false) {
      query.set('interim_results', 'false');
    }
    appendPhraseParameters(query, opts);
    appendVadParameters(query, opts.enableVad);
    const ws = new WebSocket(`${DEEPGRAM_WS}?${query.toString()}`, {
      headers: { Authorization: `Token ${apiKey}` },
    });

    const wsReady = new Promise<void>((resolve, reject) => {
      ws.once('open', () => resolve());
      ws.once('error', (err) => reject(err));
      ws.once('close', () => reject(new Error('WebSocket closed before open')));
    });
    void wsReady.catch(() => undefined);

    const listeners: {
      data: ((t: PartialTranscript) => void)[];
      error: ((err: Error) => void)[];
      close: (() => void)[];
    } = { data: [], error: [], close: [] };

    let lastMessageAt = Date.now();
    const idleTimer = setInterval(() => {
      const now = Date.now();
      if (now - lastMessageAt > STREAM_IDLE_TIMEOUT_MS) {
        const err = new Error('Deepgram websocket idle timeout');
        listeners.error.forEach((cb) => cb(err));
        closeWs();
      }
    }, Math.min(STREAM_IDLE_TIMEOUT_MS, 10_000));
    (idleTimer as NodeJS.Timeout).unref?.();

    ws.on('message', (data) => {
      lastMessageAt = Date.now();
      try {
        const json = JSON.parse(data.toString()) as DeepgramStreamingMessage;
        if (json.type === 'Results' && json.channel?.alternatives?.length) {
          const alt = json.channel.alternatives[0];
          const isFinal = Boolean(json.is_final ?? json.isFinal ?? (json as any).speech_final);
          const speakerId =
            typeof alt.speaker === 'number' || typeof alt.speaker === 'string'
              ? String(alt.speaker)
              : typeof alt.words?.[0]?.speaker === 'number' || typeof alt.words?.[0]?.speaker === 'string'
                ? String(alt.words[0].speaker)
                : undefined;
          const transcript: PartialTranscript = {
            provider: this.id,
            isFinal,
            text: alt.transcript ?? '',
            words: alt.words?.map(mapDeepgramWord),
            confidence: typeof alt.confidence === 'number' ? alt.confidence : undefined,
            timestamp: Date.now(),
            channel: 'mic',
            speakerId,
          };
          listeners.data.forEach((cb) => cb(transcript));
        } else if (json.type === 'Error') {
          const err = new Error(json.message ?? 'Deepgram error');
          listeners.error.forEach((cb) => cb(err));
        }
      } catch (error) {
        listeners.error.forEach((cb) => cb(error as Error));
      }
    });

    ws.on('error', (err) => {
      listeners.error.forEach((cb) => cb(err as Error));
    });

    ws.on('close', (code, reasonBuf) => {
      clearInterval(idleTimer);
      const reasonText = reasonBuf?.toString('utf8') || '';
      const reason = new Error(
        `Deepgram websocket closed (${code || 1000}${reasonText ? `: ${reasonText}` : ''})`
      );
      listeners.error.forEach((cb) => cb(reason));
      listeners.close.forEach((cb) => cb());
    });

    let closeScheduled = false;

    const closeWs = () => {
      if (ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
        return;
      }
      if (ws.readyState === WebSocket.CONNECTING) {
        if (closeScheduled) {
          return;
        }
        closeScheduled = true;
        ws.once('open', () => {
          closeScheduled = false;
          if (ws.readyState === WebSocket.OPEN) {
            ws.close();
          }
        });
        return;
      }
      ws.close();
    };

    async function waitForDrain() {
      while (ws.bufferedAmount > STREAM_BUFFER_HIGH_WATER_BYTES) {
        if (ws.readyState !== WebSocket.OPEN && ws.readyState !== WebSocket.CONNECTING) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }

    const controller = {
      async sendAudio(chunk: ArrayBufferLike, _meta?: { captureTs?: number }) {
        await wsReady;
        await waitForDrain();
        ws.send(Buffer.from(chunk));
      },
      async end() {
        closeWs();
      },
      async close() {
        closeWs();
      },
    };

    return {
      controller,
      onData(cb) {
        listeners.data.push(cb);
      },
      onError(cb) {
        listeners.error.push(cb);
      },
      onClose(cb) {
        listeners.close.push(cb);
      },
    };
  }

  async transcribeFileFromPCM(pcm: NodeJS.ReadableStream, opts: StreamingOptions): Promise<BatchResult> {
    const buffer = await collectStream(pcm);
    if ('destroy' in pcm && typeof (pcm as Readable).destroy === 'function') {
      (pcm as Readable).destroy();
    }
    return this.transcribePcmBuffer(buffer, opts);
  }

  async transcribePcmBuffer(buffer: Buffer, opts: StreamingOptions): Promise<BatchResult> {
    const apiKey = requireApiKey();
    const language = normalizeDeepgramLanguage(opts.language);
    const query = new URLSearchParams({
      encoding: opts.encoding,
      sample_rate: String(opts.sampleRateHz),
      language,
      punctuate: opts.punctuationPolicy === 'none' ? 'false' : 'true',
    });
    query.set('model', DEFAULT_DEEPGRAM_MODEL);
    if (DEFAULT_DEEPGRAM_TIER) {
      query.set('tier', DEFAULT_DEEPGRAM_TIER);
    }
    if (ENABLE_SMART_FORMAT) {
      query.set('smart_format', 'true');
    }
    appendPhraseParameters(query, opts);
    appendVadParameters(query, opts.enableVad);
    const contentType = `audio/l16; rate=${opts.sampleRateHz}; channels=1`;
    const url = `${DEEPGRAM_HTTP}?${query.toString()}`;
    const headers = {
      Authorization: `Token ${apiKey}`,
      'Content-Type': contentType,
    };
    const res = await this.postBuffer(buffer, url, headers);
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`Deepgram batch failed: ${res.status} ${text}`);
    }
    const json = (await res.json()) as DeepgramBatchResponse;
    return this.parseBatchResult(json);
  }

  private async postBuffer(
    buffer: Buffer,
    url: string,
    headers: Record<string, string>
  ): Promise<Response> {
    const controller = new AbortController();
    const hardTimeout = setTimeout(() => controller.abort(new Error('Deepgram batch hard timeout')), HARD_TIMEOUT_MS);
    const idleTimer = setTimeout(() => controller.abort(new Error('Deepgram batch idle timeout')), IDLE_TIMEOUT_MS);
    const requestInit: RequestInit = {
      method: 'POST',
      headers: {
        ...headers,
        'Content-Length': String(buffer.length),
      },
      body: new Uint8Array(buffer),
      signal: controller.signal,
    };
    try {
      return await fetch(url, requestInit);
    } finally {
      clearTimeout(hardTimeout);
      clearTimeout(idleTimer);
    }
  }

  private parseBatchResult(json: DeepgramBatchResponse): BatchResult {
    const alt = findFirstDeepgramAlternative(json);
    const durationRaw = json.metadata?.duration ?? alt?.duration;
    const durationSec =
      typeof durationRaw === 'number' && Number.isFinite(durationRaw) && durationRaw > 0
        ? durationRaw
        : undefined;
    const segments = extractDeepgramTranscripts(json);
    const transcriptText = segments.join(' ').trim();
    const vendorMs = json.metadata?.processing_ms ?? json.metadata?.processing_time;
    const vendorProcessingMs =
      typeof vendorMs === 'number' && Number.isFinite(vendorMs) && vendorMs > 0
        ? Math.round(vendorMs)
        : undefined;
    return {
      provider: this.id,
      text: transcriptText,
      words: alt?.words?.map(mapDeepgramWord),
      durationSec,
      vendorProcessingMs,
    };
  }
}
