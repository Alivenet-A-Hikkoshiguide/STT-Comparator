import type { RawData, WebSocket } from 'ws';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { loadConfig } from '../config.js';
import type { AppConfig } from '../config.js';
import { getAdapter } from '../adapters/index.js';
import { logger } from '../logger.js';
import { bufferToArrayBuffer } from '../utils/buffer.js';
import { parseStreamFrame } from '../utils/streamHeader.js';
import { createPcmResampler } from '../utils/ffmpeg.js';
import { getProviderSampleRate } from '../utils/providerAudio.js';
import { createMeetingAudioGate } from '../utils/meetingGate.js';
import { voiceCommandMessageSchema, voiceConfigMessageSchema } from '../validation.js';
import type {
  ProviderId,
  StreamingController,
  VoiceAssistantAudioEndMessage,
  VoiceAssistantAudioStartMessage,
  VoiceAssistantTextMessage,
  VoiceCommandMessage,
  VoiceConfigMessage,
  VoiceInputSource,
  VoiceServerMessage,
  VoiceSessionMessage,
  VoiceState,
  VoiceStateMessage,
  VoiceUserTranscriptMessage,
} from '../types.js';
import { generateChatReply } from '../voice/openaiResponses.js';
import { resolveVoicePreset } from '../voice/voicePresets.js';
import { createTtsStream } from '../voice/ttsPipeline.js';
import type { OpenAiRealtimeVoiceSession } from '../voice/openaiRealtimeVoice.js';
import { startOpenAiRealtimeVoiceSession } from '../voice/openaiRealtimeVoice.js';

type ChatMessage = { role: 'system' | 'user' | 'assistant'; content: string };

function getBaseSystemPrompt(): string {
  return (
    process.env.VOICE_SYSTEM_PROMPT ??
    'あなたは日本語で会話する音声アシスタントです。簡潔で自然な日本語で答えてください。'
  );
}

function resolveVoiceMemoryPath(config: AppConfig): string | null {
  const envPath = process.env.VOICE_MEMORY_PATH?.trim();
  if (envPath) return envPath;
  const configPath = config.voice?.memoryPath?.trim();
  return configPath || null;
}

async function loadVoiceMemory(config: AppConfig): Promise<string | null> {
  const memoryPath = resolveVoiceMemoryPath(config);
  if (!memoryPath) return null;
  const resolved = path.resolve(memoryPath);
  let raw: string;
  try {
    raw = await readFile(resolved, 'utf-8');
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`failed to read voice memory (${resolved}): ${message}`);
  }
  const trimmed = raw.trim();
  if (!trimmed) {
    logger.warn({ event: 'voice_memory_empty', path: resolved });
    return null;
  }
  return trimmed;
}

function buildSystemPrompt(basePrompt: string, memoryText: string | null): string {
  if (!memoryText) return basePrompt;
  return `${basePrompt}\n\n[事前記憶]\n${memoryText}`;
}

function getHistoryMaxTurns(): number {
  const raw = Number(process.env.VOICE_HISTORY_MAX_TURNS);
  if (Number.isFinite(raw) && raw >= 1) return Math.min(50, Math.round(raw));
  return 12;
}

function stripWakeWordPrefix(text: string, wakeWords: readonly string[]): { matched: boolean; cleaned: string } {
  const trimmed = text.trim();
  if (!trimmed) return { matched: false, cleaned: trimmed };
  const lower = trimmed.toLowerCase();
  for (const w of wakeWords) {
    const token = w.trim();
    if (!token) continue;
    const lowerToken = token.toLowerCase();
    if (!lower.startsWith(lowerToken)) continue;
    // For ASCII tokens, require a word boundary after the wake word (e.g., "ai" should not match "aiden").
    if (/^[a-z0-9]+$/i.test(token)) {
      const nextChar = trimmed.slice(token.length, token.length + 1);
      if (nextChar && /[a-z0-9]/i.test(nextChar)) {
        continue;
      }
    }
    const rest = trimmed.slice(token.length).replace(/^[\s,、:：-]+/, '').trim();
    return { matched: true, cleaned: rest };
  }
  return { matched: false, cleaned: trimmed };
}

function defaultWakeWords(lang: string): string[] {
  const lower = lang.toLowerCase();
  if (lower.startsWith('ja')) {
    return ['アシスタント', 'assistant', 'AI'];
  }
  return ['assistant', 'ai'];
}

const DEFAULT_MEETING_OPEN_WINDOW_MS = 6000;
const DEFAULT_MEETING_COOLDOWN_MS = 1500;
const DEFAULT_MEETING_ECHO_SUPPRESS_MS = 3000;
const DEFAULT_MEETING_ECHO_SIMILARITY = 0.8;
const DEFAULT_MEETING_INTRO_TEXT_JA = '質問は「アシスタント」と呼びかけてから続けて話してください。';
const DEFAULT_MEETING_INTRO_TEXT_EN = 'Please say "assistant" and then continue your question.';

const clampNumber = (value: number | undefined, min: number, max: number, fallback: number): number => {
  const safe = typeof value === 'number' && Number.isFinite(value) ? value : fallback;
  return Math.min(max, Math.max(min, safe));
};

const clampInt = (value: number | undefined, min: number, max: number, fallback: number): number => {
  const safe = typeof value === 'number' && Number.isFinite(value) ? Math.round(value) : fallback;
  return Math.min(max, Math.max(min, safe));
};

const normalizeErrorCause = (cause: unknown): unknown => {
  if (!cause) return undefined;
  if (cause instanceof Error) {
    return { name: cause.name, message: cause.message, stack: cause.stack };
  }
  return cause;
};

const coerceError = (err: unknown, fallback: string): Error => {
  if (err instanceof Error) return err;
  if (typeof err === 'string' && err.trim()) return new Error(err);
  return new Error(fallback);
};

const logVoiceError = (event: 'voice_agent_error' | 'voice_intro_error', err: unknown, fallback: string): Error => {
  const error = coerceError(err, fallback);
  logger.error({
    event,
    message: error.message,
    name: error.name,
    cause: normalizeErrorCause(error.cause),
    stack: error.stack,
  });
  return error;
};

const normalizeEchoText = (text: string): string =>
  text.normalize('NFKC').toLowerCase().replace(/[^\p{L}\p{N}]+/gu, '');

const buildBigrams = (text: string): Set<string> => {
  const set = new Set<string>();
  if (!text) return set;
  if (text.length <= 2) {
    set.add(text);
    return set;
  }
  for (let i = 0; i < text.length - 1; i += 1) {
    set.add(text.slice(i, i + 2));
  }
  return set;
};

const jaccardSimilarity = (a: string, b: string): number => {
  if (!a || !b) return 0;
  if (a === b) return 1;
  const setA = buildBigrams(a);
  const setB = buildBigrams(b);
  if (setA.size === 0 || setB.size === 0) return 0;
  let intersection = 0;
  for (const token of setA) {
    if (setB.has(token)) intersection += 1;
  }
  return intersection / (setA.size + setB.size - intersection);
};

export async function handleVoiceConnection(ws: WebSocket, lang: string) {
  const config = await loadConfig();
  const ttsConfig = config.voice?.tts;
  const voiceVad = config.voice?.vad;
  const meetingGate = createMeetingAudioGate(config.voice?.meetingGate);
  const sessionId = randomUUID();
  const startedAt = new Date().toISOString();
  const keepaliveMs = config.ws?.keepaliveMs ?? 30_000;
  const maxMissedPongs = config.ws?.maxMissedPongs ?? 2;
  const shouldKeepalive = keepaliveMs > 0 && maxMissedPongs > 0;
  let outputSampleRate = config.audio.targetSampleRate ?? 16_000;
  const openAiRealtimeSampleRate = 24_000;
  let presetId: string | null = null;
  let presetMode: 'pipeline' | 'openai_realtime' = 'pipeline';
  let sttProvider: ProviderId | null = null;
  let ttsProvider: ProviderId | null = null;
  const llmProvider = 'openai' as const;

  let closed = false;
  let keepaliveTimer: NodeJS.Timeout | null = null;
  let missedPongs = 0;
  let sessionStarted = false;
  let voiceState: VoiceState = 'listening';
  let finalizeDelayMs = 200;

  const baseSystemPrompt = getBaseSystemPrompt();
  let systemPrompt = baseSystemPrompt;
  const historyMaxTurns = getHistoryMaxTurns();
  const history: ChatMessage[] = [{ role: 'system', content: systemPrompt }];
  let memoryLoaded = false;
  let memoryLoadPromise: Promise<void> | null = null;
  const ensureVoiceMemoryLoaded = async () => {
    if (memoryLoaded) return;
    if (!memoryLoadPromise) {
      memoryLoadPromise = (async () => {
        const memoryText = await loadVoiceMemory(config);
        systemPrompt = buildSystemPrompt(baseSystemPrompt, memoryText);
        history[0] = { role: 'system', content: systemPrompt };
        memoryLoaded = true;
      })();
    }
    await memoryLoadPromise;
  };
  const trimHistory = () => {
    while (history.length > 1 + historyMaxTurns * 2) {
      history.splice(1, 2);
    }
  };
  const pendingFinalParts: string[] = [];
  let finalizeTimer: NodeJS.Timeout | null = null;

  // Transcripts captured while assistant is speaking (applied only when barge-in happens).
  type TranscriptInfo = {
    displayText: string;
    triggerText: string | null;
    triggered: boolean;
  };
  type ResolvedTranscript = {
    info: TranscriptInfo;
    wakeWordMatched: boolean;
    cleanedText: string;
    openWindowActive: boolean;
  };
  type SuppressedTranscript = { source: VoiceInputSource; info: TranscriptInfo; speakerId?: string };
  let suppressedInterim: SuppressedTranscript | null = null;
  const suppressedFinalParts: SuppressedTranscript[] = [];
  const clearSuppressedTranscripts = () => {
    suppressedInterim = null;
    suppressedFinalParts.length = 0;
  };

  const resamplers = new Map<VoiceInputSource, ReturnType<typeof createPcmResampler>>();
  let clientSampleRate = outputSampleRate;
  let messageChain: Promise<void> = Promise.resolve();

  let assistantTurn:
    | {
        turnId: string;
        abort: AbortController;
        state: 'thinking' | 'speaking';
      }
    | null = null;

  const sttControllers = new Map<VoiceInputSource, StreamingController>();
  const sttPendings = new Map<VoiceInputSource, Promise<void>>();
  let realtimePending: Promise<void> | null = null;

  let realtimeSession: OpenAiRealtimeVoiceSession | null = null;
  let realtimeActive:
    | {
        responseId: string;
        turnId: string;
        createdAtMs: number;
        audioStarted: boolean;
        audioEndSent: boolean;
        audioDone: boolean;
        audioItemId?: string;
        audioBytesSent: number;
        cancelReason?: 'barge_in' | 'stopped';
        ignoreOutput?: boolean;
        assistantTextSent: boolean;
        assistantTextDone: boolean;
      }
    | null = null;

  const sendJson = (payload: VoiceServerMessage | { type: 'ping'; ts: number }) => {
    try {
      ws.send(JSON.stringify(payload));
    } catch {
      // ignore send failures; close handlers will clean up
    }
  };

  const sendState = (state: VoiceState, turnId?: string) => {
    voiceState = state;
    const msg: VoiceStateMessage = { type: 'voice_state', state, ts: Date.now(), turnId };
    sendJson(msg);
  };

  const sendAudioStart = (turnId: string, metrics?: { llmMs?: number; ttsTtfbMs?: number }) => {
    const msg: VoiceAssistantAudioStartMessage = {
      type: 'voice_assistant_audio_start',
      turnId,
      timestamp: Date.now(),
      llmMs: metrics?.llmMs,
      ttsTtfbMs: metrics?.ttsTtfbMs,
    };
    sendJson(msg);
  };

  const sendAudioEnd = (turnId: string, reason?: VoiceAssistantAudioEndMessage['reason']) => {
    const msg: VoiceAssistantAudioEndMessage = {
      type: 'voice_assistant_audio_end',
      turnId,
      timestamp: Date.now(),
      reason,
    };
    sendJson(msg);
  };

  const recordAssistantText = (text: string) => {
    const normalized = normalizeEchoText(text);
    lastAssistantText = { text, normalized, ts: Date.now() };
  };

  const sendMeetingWindow = (
    state: 'opened' | 'closed',
    opts?: { expiresAt?: number; reason?: 'wake_word' | 'timeout' | 'manual' | 'cooldown' }
  ) => {
    sendJson({
      type: 'voice_meeting_window',
      state,
      ts: Date.now(),
      expiresAt: opts?.expiresAt,
      reason: opts?.reason,
    });
  };

  const closeMeetingWindow = (now: number, reason: 'timeout' | 'manual' | 'cooldown' = 'timeout') => {
    if (!meetingWindowOpen) return;
    meetingWindowOpen = false;
    meetingOpenUntil = 0;
    if (meetingCooldownMs > 0) {
      meetingCooldownUntil = now + meetingCooldownMs;
    }
    sendMeetingWindow('closed', { reason });
    logger.info({ event: 'voice_meeting_window_closed', sessionId, reason });
  };

  const openMeetingWindow = (now: number, reason: 'wake_word' | 'manual' = 'wake_word') => {
    const canOpen = meetingOpenWindowMs > 0;
    if (!canOpen) return;
    const wasOpen = meetingOpenUntil > now;
    if (meetingCooldownMs > 0) {
      meetingCooldownUntil = now + meetingCooldownMs;
    }
    meetingOpenUntil = now + meetingOpenWindowMs;
    if (!wasOpen) {
      meetingWindowOpen = true;
      sendMeetingWindow('opened', { expiresAt: meetingOpenUntil, reason });
      logger.info({ event: 'voice_meeting_window_opened', sessionId, expiresAt: meetingOpenUntil, reason });
    }
  };

  const refreshMeetingWindow = (now: number) => {
    if (meetingOpenUntil > 0 && now >= meetingOpenUntil) {
      closeMeetingWindow(now, 'timeout');
    }
  };

  const extendMeetingWindow = (now: number) => {
    if (meetingOpenWindowMs <= 0) return;
    if (meetingOpenUntil > now) {
      meetingOpenUntil = Math.max(meetingOpenUntil, now + meetingOpenWindowMs);
    }
  };

  const isEchoLikely = (text: string, altText?: string, now = Date.now()) => {
    if (!meetingModeEnabled || !meetingOutputEnabled) return false;
    if (meetingEchoSuppressMs <= 0 || meetingEchoSimilarity <= 0) return false;
    const last = lastAssistantText;
    if (!last?.normalized) return false;
    if (now - last.ts > meetingEchoSuppressMs) return false;
    const normalized = normalizeEchoText(text);
    const altNormalized = altText ? normalizeEchoText(altText) : '';
    const base = last.normalized;
    const candidates = [normalized];
    if (altNormalized && altNormalized !== normalized) {
      candidates.push(altNormalized);
    }
    for (const candidate of candidates) {
      if (!candidate) continue;
      if (candidate.length < 4 || base.length < 4) {
        if (candidate === base) return true;
        continue;
      }
      const similarity = jaccardSimilarity(candidate, base);
      if (similarity >= meetingEchoSimilarity) {
        return true;
      }
    }
    return false;
  };

  const sendUserTranscript = (
    source: VoiceInputSource,
    info: TranscriptInfo,
    isFinal: boolean,
    speakerId?: string
  ) => {
    const msg: VoiceUserTranscriptMessage = {
      type: 'voice_user_transcript',
      isFinal,
      text: info.displayText,
      timestamp: Date.now(),
      source,
      speakerId,
      triggered: info.triggered,
    };
    sendJson(msg);
  };

  const stopAssistant = (
    reason: 'barge_in' | 'stopped' | 'error',
    opts?: { playedMs?: number; cancelRealtime?: boolean }
  ) => {
    if (presetMode === 'openai_realtime') {
      const active = realtimeActive;
      if (!active) {
        sendState('listening');
        return;
      }

      const endReason: VoiceAssistantAudioEndMessage['reason'] =
        reason === 'barge_in' ? 'barge_in' : reason === 'stopped' ? 'stopped' : 'error';

      active.cancelReason = reason === 'barge_in' ? 'barge_in' : reason === 'stopped' ? 'stopped' : active.cancelReason;
      active.ignoreOutput = true;

      // Best-effort truncate so the conversation state matches what the user actually heard.
      if (active.audioItemId && active.audioBytesSent > 0) {
        const maxAudioEndMs = (active.audioBytesSent / (openAiRealtimeSampleRate * 2)) * 1000;
        const playedMs = opts?.playedMs;
        const audioEndMs =
          typeof playedMs === 'number' && Number.isFinite(playedMs)
            ? Math.min(Math.max(0, playedMs), maxAudioEndMs)
            : maxAudioEndMs;
        void realtimeSession
          ?.truncateOutputAudio(active.audioItemId, audioEndMs)
          .catch((err) => logger.warn({ event: 'openai_realtime_truncate_failed', message: (err as Error).message }));
      }

      const shouldCancel = opts?.cancelRealtime !== false && reason !== 'error';
      if (shouldCancel) {
        void realtimeSession
          ?.cancelResponse()
          .catch((err) => logger.warn({ event: 'openai_realtime_cancel_failed', message: (err as Error).message }));
      }

      if (active.audioStarted && !active.audioEndSent) {
        sendAudioEnd(active.turnId, endReason);
        active.audioEndSent = true;
      }

      sendState('listening');
      return;
    }

    const active = assistantTurn;
    if (!active) return;
    try {
      active.abort.abort();
    } catch {
      // ignore
    }
    const preserveSuppressed = reason === 'barge_in' && active.state === 'speaking';
    if (!preserveSuppressed) {
      clearSuppressedTranscripts();
    }
    if (active.state === 'speaking') {
      sendAudioEnd(active.turnId, reason);
    }
    assistantTurn = null;
    sendState('listening');
  };

  const scheduleFinalize = () => {
    if (finalizeTimer) clearTimeout(finalizeTimer);
    finalizeTimer = setTimeout(() => {
      void finalizeUserTurn();
    }, finalizeDelayMs);
  };

  const applySuppressedTranscripts = () => {
    if (suppressedFinalParts.length === 0 && !suppressedInterim) return;
    if (suppressedFinalParts.length > 0) {
      const parts = suppressedFinalParts.splice(0);
      let shouldFinalize = false;
      for (const part of parts) {
        sendUserTranscript(part.source, part.info, true, part.speakerId);
        if (part.info.triggerText) {
          enqueueFinalPart(part.info.triggerText);
          shouldFinalize = true;
        }
      }
      if (shouldFinalize) {
        scheduleFinalize();
      }
    } else if (suppressedInterim) {
      sendUserTranscript(suppressedInterim.source, suppressedInterim.info, false, suppressedInterim.speakerId);
    }
    clearSuppressedTranscripts();
  };

  const finalizeUserTurn = async () => {
    if (closed) return;
    if (assistantTurn) return;
    const text = pendingFinalParts.join(' ').trim();
    pendingFinalParts.length = 0;
    if (!text) return;

    const abort = new AbortController();
    const turnId = randomUUID();
    assistantTurn = { turnId, abort, state: 'thinking' };
    sendState('thinking', turnId);
    const isActiveTurn = () => assistantTurn?.turnId === turnId;

    history.push({ role: 'user', content: text });
    trimHistory();

    try {
      const llmStart = Date.now();
      const reply = await generateChatReply(history, {
        signal: abort.signal,
        instructions: history[0]?.content,
      });
      const rawAssistantText = reply.text;
      const leadingWhitespace = rawAssistantText.match(/^\s+/)?.[0].length ?? 0;
      const trimmed = rawAssistantText.trim();
      let citations = reply.citations;
      if (leadingWhitespace > 0 || trimmed.length !== rawAssistantText.length) {
        const trimmedLength = trimmed.length;
        citations = citations
          .map((citation) => ({
            ...citation,
            startIndex: citation.startIndex - leadingWhitespace,
            endIndex: citation.endIndex - leadingWhitespace,
          }))
          .filter((citation) => citation.startIndex >= 0 && citation.endIndex <= trimmedLength);
      }

      const assistantText = sanitizeAssistantText(trimmed);
      if (assistantText !== trimmed && citations.length > 0) {
        citations = [];
      }
      const llmMs = Date.now() - llmStart;

      if (abort.signal.aborted || closed) {
        return;
      }
      if (!isActiveTurn()) {
        return;
      }
      const provider = ttsProvider;
      if (!provider) {
        throw new Error('voice session is not initialized (missing tts provider)');
      }

      history.push({ role: 'assistant', content: assistantText });
      trimHistory();
      const textMsg: VoiceAssistantTextMessage = {
        type: 'voice_assistant_text',
        turnId,
        text: assistantText,
        citations: citations.length > 0 ? citations : undefined,
        isFinal: true,
        timestamp: Date.now(),
      };
      sendJson(textMsg);
      recordAssistantText(assistantText);

      const ttsStart = Date.now();
      let ttsTtfbMs: number | null = null;
      let audioChunks = 0;
      let audioBytes = 0;
      let startedSpeaking = false;

      const { stream: ttsStream, stats: ttsStats } = createTtsStream({
        provider,
        text: assistantText,
        lang,
        sampleRate: outputSampleRate,
        signal: abort.signal,
        config: ttsConfig,
      });

      for await (const pcm of ttsStream) {
        if (abort.signal.aborted || closed) break;
        if (!isActiveTurn()) break;
        if (!startedSpeaking) {
          startedSpeaking = true;
          ttsTtfbMs = Date.now() - ttsStart;
          clearSuppressedTranscripts();
          assistantTurn = { turnId, abort, state: 'speaking' };
          sendState('speaking', turnId);
          sendAudioStart(turnId, { llmMs, ttsTtfbMs });
        }
        try {
          ws.send(pcm);
          audioChunks += 1;
          audioBytes += pcm.length;
        } catch {
          break;
        }
      }

      if (!abort.signal.aborted && !closed && isActiveTurn() && startedSpeaking) {
        sendAudioEnd(turnId, 'completed');
      }

      if (!abort.signal.aborted && !closed && isActiveTurn()) {
        logger.info({
          event: 'voice_turn_metrics',
          sessionId,
          turnId,
          presetId: presetId ?? undefined,
          sttProvider: sttProvider ?? undefined,
          ttsProvider: ttsProvider ?? undefined,
          lang,
          llmMs,
          ttsTtfbMs,
          ttsMs: Date.now() - ttsStart,
          audioChunks,
          audioBytes,
          outputSampleRate,
          ttsStrategy: ttsStats.strategy,
          ttsSegments: ttsStats.segments,
          ttsRetries: ttsStats.retries,
        });
      }
    } catch (err) {
      if (abort.signal.aborted || closed) {
        return;
      }
      if (!isActiveTurn()) {
        return;
      }
      const error = logVoiceError('voice_agent_error', err, 'voice agent error');
      if (assistantTurn?.state === 'speaking') {
        sendAudioEnd(turnId, 'error');
      }
      sendJson({ type: 'error', message: error.message });
    } finally {
      if (isActiveTurn()) {
        assistantTurn = null;
        clearSuppressedTranscripts();
        if (!closed) {
          sendState('listening');
        }
      }
    }
  };

  const speakAssistantText = async (assistantText: string) => {
    if (closed) return;
    if (assistantTurn) return;
    const provider = ttsProvider;
    if (!provider) return;
    const trimmed = assistantText.trim();
    if (!trimmed) return;

    const abort = new AbortController();
    const turnId = randomUUID();
    assistantTurn = { turnId, abort, state: 'thinking' };
    sendState('thinking', turnId);
    const isActiveTurn = () => assistantTurn?.turnId === turnId;

    const textMsg: VoiceAssistantTextMessage = {
      type: 'voice_assistant_text',
      turnId,
      text: trimmed,
      isFinal: true,
      timestamp: Date.now(),
    };
    sendJson(textMsg);
    recordAssistantText(trimmed);

    const ttsStart = Date.now();
    let ttsTtfbMs: number | null = null;
    let audioChunks = 0;
    let audioBytes = 0;
    let startedSpeaking = false;

    try {
      const { stream: ttsStream, stats: ttsStats } = createTtsStream({
        provider,
        text: trimmed,
        lang,
        sampleRate: outputSampleRate,
        signal: abort.signal,
        config: ttsConfig,
      });

      for await (const pcm of ttsStream) {
        if (abort.signal.aborted || closed) break;
        if (!isActiveTurn()) break;
        if (!startedSpeaking) {
          startedSpeaking = true;
          ttsTtfbMs = Date.now() - ttsStart;
          clearSuppressedTranscripts();
          assistantTurn = { turnId, abort, state: 'speaking' };
          sendState('speaking', turnId);
          sendAudioStart(turnId, { ttsTtfbMs });
        }
        try {
          ws.send(pcm);
          audioChunks += 1;
          audioBytes += pcm.length;
        } catch {
          break;
        }
      }

      if (!abort.signal.aborted && !closed && isActiveTurn() && startedSpeaking) {
        sendAudioEnd(turnId, 'completed');
      }

      if (!abort.signal.aborted && !closed && isActiveTurn()) {
        logger.info({
          event: 'voice_intro_metrics',
          sessionId,
          turnId,
          presetId: presetId ?? undefined,
          ttsProvider: ttsProvider ?? undefined,
          lang,
          ttsTtfbMs,
          ttsMs: Date.now() - ttsStart,
          audioChunks,
          audioBytes,
          outputSampleRate,
          ttsStrategy: ttsStats.strategy,
          ttsSegments: ttsStats.segments,
          ttsRetries: ttsStats.retries,
        });
      }
    } catch (err) {
      if (abort.signal.aborted || closed) {
        return;
      }
      if (!isActiveTurn()) {
        return;
      }
      const error = logVoiceError('voice_intro_error', err, 'voice agent error');
      if (assistantTurn?.state === 'speaking') {
        sendAudioEnd(turnId, 'error');
      }
      sendJson({ type: 'error', message: error.message });
    } finally {
      if (isActiveTurn()) {
        assistantTurn = null;
        clearSuppressedTranscripts();
        if (!closed) {
          sendState('listening');
        }
      }
    }
  };

  const ensureResampler = (source: VoiceInputSource, inputSampleRate: number) => {
    if (inputSampleRate === outputSampleRate) return null;
    const existing = resamplers.get(source);
    if (existing) return existing;
    const created = createPcmResampler({
      inputSampleRate,
      outputSampleRate,
      channels: 1,
    });
    created.onChunk((chunk, meta) => {
      void handlePcmChunk(source, chunk, meta);
    });
    created.onError((err) => handleFatal(err));
    created.onClose((code) => {
      if (closed) return;
      if (typeof code === 'number' && code !== 0) {
        handleFatal(new Error(`ffmpeg exited with code ${code}`));
      }
    });
    resamplers.set(source, created);
    return created;
  };

  async function sendToStt(source: VoiceInputSource, pcm: Buffer) {
    const controller = sttControllers.get(source);
    if (!controller) return;
    const prev = sttPendings.get(source) ?? Promise.resolve();
    const sendPromise = prev
      .then(async () => {
        await controller.sendAudio(bufferToArrayBuffer(pcm));
      })
      .catch((err) => handleFatal(err as Error));
    sttPendings.set(source, sendPromise);
    await sendPromise;
  }

  async function sendToRealtime(pcm: Buffer) {
    if (!realtimeSession) return;
    const sendPromise = (realtimePending ?? Promise.resolve())
      .then(async () => {
        await realtimeSession?.appendAudio(pcm);
      })
      .catch((err) => handleFatal(err as Error));
    realtimePending = sendPromise;
    await sendPromise;
  }

  async function sendAudioToBackend(source: VoiceInputSource, pcm: Buffer) {
    if (presetMode === 'openai_realtime') {
      await sendToRealtime(pcm);
      return;
    }
    await sendToStt(source, pcm);
  }

  function handleFatal(err: Error) {
    if (closed) return;
    closed = true;
    if (keepaliveTimer) {
      clearInterval(keepaliveTimer);
      keepaliveTimer = null;
    }
    stopAssistant('error');
    sendJson({ type: 'error', message: err.message });
    ws.close();
  }

  let meetingModeEnabled = false;
  let meetingRequireWakeWord = false;
  let meetingOutputEnabled = false;
  let wakeWords: string[] = [];
  let meetingOpenUntil = 0;
  let meetingCooldownUntil = 0;
  let meetingOpenWindowMs = DEFAULT_MEETING_OPEN_WINDOW_MS;
  let meetingCooldownMs = DEFAULT_MEETING_COOLDOWN_MS;
  let meetingEchoSuppressMs = DEFAULT_MEETING_ECHO_SUPPRESS_MS;
  let meetingEchoSimilarity = DEFAULT_MEETING_ECHO_SIMILARITY;
  let meetingIntroEnabled = true;
  let meetingIntroText: string | null = null;
  let meetingWindowOpen = false;
  let meetingIntroSent = false;
  let lastAssistantText: { text: string; normalized: string; ts: number } | null = null;

  async function handlePcmChunk(
    source: VoiceInputSource,
    pcm: Buffer,
    meta?: { captureTs?: number; durationMs?: number }
  ) {
    if (source === 'meeting' && meetingModeEnabled && meetingGate.config.enabled) {
      const decision = meetingGate.shouldForward(pcm, {
        captureTs: meta?.captureTs,
        durationMs: meta?.durationMs,
        assistantSpeaking: meetingOutputEnabled && voiceState === 'speaking',
        sampleRateHz: outputSampleRate,
      });
      if (!decision.allow) return;
    }
    await sendAudioToBackend(source, pcm);
  }

  const meetingSystemPromptSuffix =
    '\n\nあなたはWeb会議に参加しています。会議の妨げにならないよう、簡潔に返答してください。';

  const applyMeetingSystemPrompt = () => {
    if (!meetingModeEnabled) return;
    const currentSystem = history[0]?.content ?? systemPrompt;
    history[0] = {
      role: 'system',
      content: currentSystem.includes('あなたはWeb会議に参加しています。')
        ? currentSystem
        : `${currentSystem}${meetingSystemPromptSuffix}`,
    };
  };

  const sanitizeAssistantText = (text: string): string => {
    if (!meetingModeEnabled) return text;
    return text.replace(/(^|\n)\s*\[(meeting|me)\]\s*/gi, '$1').trim();
  };

  const buildTranscriptInfo = (source: VoiceInputSource, text: string, now: number): ResolvedTranscript | null => {
    const displayText = text.trim();
    if (!displayText) return null;
    if (source !== 'meeting' || !meetingRequireWakeWord) {
      return {
        info: { displayText, triggerText: displayText, triggered: true },
        wakeWordMatched: false,
        cleanedText: displayText,
        openWindowActive: false,
      };
    }
    const stripped = stripWakeWordPrefix(displayText, wakeWords);
    const cleaned = stripped.cleaned.trim();
    const cleanedText = cleaned;
    const cooldownBlocked = meetingOpenWindowMs > 0 && now < meetingCooldownUntil;
    const wakeWordMatched = stripped.matched && !cooldownBlocked;
    const openWindowAllowed = meetingOpenWindowMs > 0;
    const openWindowActive = meetingOpenUntil > now || (openWindowAllowed && wakeWordMatched);
    if (wakeWordMatched) {
      const hasContent = /[\p{L}\p{N}]/u.test(cleanedText);
      return {
        info: { displayText, triggerText: hasContent ? cleanedText : null, triggered: hasContent },
        wakeWordMatched: true,
        cleanedText,
        openWindowActive,
      };
    }
    if (openWindowActive) {
      return {
        info: { displayText, triggerText: displayText, triggered: true },
        wakeWordMatched: false,
        cleanedText: displayText,
        openWindowActive,
      };
    }
    return {
      info: { displayText, triggerText: null, triggered: false },
      wakeWordMatched: false,
      cleanedText: displayText,
      openWindowActive: false,
    };
  };

  const enqueueFinalPart = (text: string) => {
    if (!text) return false;
    pendingFinalParts.push(text);
    return true;
  };

  const startOrRestartRealtimeSession = async () => {
    const prev = realtimeSession;
    realtimeSession = null;
    realtimeActive = null;
    realtimePending = null;
    await prev?.close().catch(() => undefined);

    const realtime = startOpenAiRealtimeVoiceSession(
      {
        lang,
        systemPrompt,
        vad: voiceVad,
      },
      {
        onSpeechStarted: () => {
          if (closed) return;
          if (voiceState === 'speaking') {
            stopAssistant('barge_in', { cancelRealtime: false });
          }
        },
        onUserTranscript: ({ text, isFinal }) => {
          if (closed) return;
          const trimmed = text.trim();
          if (!trimmed) return;
          const msgPayload: VoiceUserTranscriptMessage = {
            type: 'voice_user_transcript',
            isFinal,
            text: trimmed,
            timestamp: Date.now(),
            source: 'mic',
          };
          sendJson(msgPayload);
        },
        onResponseCreated: ({ responseId }) => {
          if (closed) return;
          // Close out any in-flight turn container so the UI doesn't get stuck.
          if (realtimeActive?.audioStarted && !realtimeActive.audioEndSent) {
            realtimeActive.audioEndSent = true;
            sendAudioEnd(realtimeActive.turnId, realtimeActive.cancelReason ?? 'stopped');
          }
          const turnId = randomUUID();
          realtimeActive = {
            responseId,
            turnId,
            createdAtMs: Date.now(),
            audioStarted: false,
            audioEndSent: false,
            audioDone: false,
            audioBytesSent: 0,
            ignoreOutput: false,
            assistantTextSent: false,
            assistantTextDone: false,
          };
          sendState('thinking', turnId);
        },
        onResponseDone: ({ responseId, status }) => {
          if (closed) return;
          const active = realtimeActive;
          if (!active || active.responseId !== responseId) return;

          const normalizedStatus = (status ?? '').toLowerCase();
          const reason: VoiceAssistantAudioEndMessage['reason'] =
            active.cancelReason ??
            (normalizedStatus === 'failed' || normalizedStatus === 'incomplete'
              ? 'error'
              : normalizedStatus === 'cancelled'
                ? 'stopped'
                : 'completed');

          if (active.audioStarted && !active.audioEndSent) {
            active.audioEndSent = true;
            sendAudioEnd(active.turnId, reason);
          }
          sendState('listening');
          realtimeActive = null;
        },
        onAssistantAudioDelta: ({ responseId, itemId, pcm }) => {
          if (closed) return;
          let active = realtimeActive;
          if (!active) {
            // Fallback: if we missed response.created, synthesize a turn container.
            const turnId = randomUUID();
            active = {
              responseId,
              turnId,
              createdAtMs: Date.now(),
              audioStarted: false,
              audioEndSent: false,
              audioDone: false,
              audioBytesSent: 0,
              ignoreOutput: false,
              assistantTextSent: false,
              assistantTextDone: false,
            };
            realtimeActive = active;
            sendState('thinking', turnId);
          }
          if (active.responseId !== responseId) return;
          if (active.ignoreOutput) return;

          if (itemId && !active.audioItemId) {
            active.audioItemId = itemId;
          }

          if (!active.audioStarted) {
            active.audioStarted = true;
            const ttsTtfbMs = Date.now() - active.createdAtMs;
            clearSuppressedTranscripts();
            sendState('speaking', active.turnId);
            sendAudioStart(active.turnId, { ttsTtfbMs });
          }

          try {
            ws.send(pcm);
            active.audioBytesSent += pcm.length;
          } catch {
            // ignore send failures; close handlers will clean up
          }
        },
        onAssistantAudioDone: ({ responseId }) => {
          if (closed) return;
          const active = realtimeActive;
          if (!active || active.responseId !== responseId) return;

          active.audioDone = true;
          if (active.audioStarted && !active.audioEndSent) {
            active.audioEndSent = true;
            sendAudioEnd(active.turnId, active.cancelReason ?? 'completed');
          }
          sendState('listening');

          if (active.assistantTextDone) {
            realtimeActive = null;
          }
        },
        onAssistantTextDone: ({ responseId, text }) => {
          if (closed) return;
          const active = realtimeActive;
          if (!active || active.responseId !== responseId) return;
          if (active.ignoreOutput) return;
          const sanitized = sanitizeAssistantText(text.trim());
          if (!sanitized) return;
          if (!active.assistantTextSent) {
            active.assistantTextSent = true;
            const textMsg: VoiceAssistantTextMessage = {
              type: 'voice_assistant_text',
              turnId: active.turnId,
              text: sanitized,
              isFinal: true,
              timestamp: Date.now(),
            };
            sendJson(textMsg);
            recordAssistantText(sanitized);
          }
          active.assistantTextDone = true;
          if (active.audioDone) {
            realtimeActive = null;
          }
        },
        onError: (err) => handleFatal(err),
        onClose: () => {
          if (closed) return;
          handleFatal(new Error('openai realtime voice session closed'));
        },
      }
    );

    realtimeSession = realtime;
    await realtime.ready;
  };

  const startStt = async (msg: VoiceConfigMessage) => {
    clientSampleRate = msg.clientSampleRate;
    finalizeDelayMs = msg.options?.finalizeDelayMs ?? finalizeDelayMs;
    const enableInterim = msg.enableInterim !== false;

    await ensureVoiceMemoryLoaded();

    const preset = resolveVoicePreset(config, msg.presetId);
    if (!preset.available) {
      const detail = [...preset.missingEnv, ...preset.issues].filter(Boolean).join(', ');
      throw new Error(`voice preset unavailable (${preset.id}): ${detail || 'unknown reason'}`);
    }

    presetId = preset.id;
    presetMode = preset.mode;
    const selectedSttProvider = preset.providers.stt;
    const selectedTtsProvider = preset.providers.tts;
    sttProvider = selectedSttProvider;
    ttsProvider = selectedTtsProvider;

    meetingModeEnabled = msg.options?.meetingMode === true || (msg.channelSplit === true && msg.channels === 2);
    meetingGate.reset();
    meetingOutputEnabled = meetingModeEnabled && msg.options?.meetingOutputEnabled === true;
    wakeWords = (msg.options?.wakeWords ?? []).map((w) => w.trim()).filter(Boolean);
    if (wakeWords.length === 0) {
      wakeWords = defaultWakeWords(lang);
    }
    meetingRequireWakeWord = msg.options?.meetingRequireWakeWord ?? meetingModeEnabled;
    const meetingConfig = config.voice?.meeting;
    meetingOpenWindowMs = clampInt(
      msg.options?.meetingOpenWindowMs ?? meetingConfig?.openWindowMs,
      0,
      30_000,
      DEFAULT_MEETING_OPEN_WINDOW_MS
    );
    meetingCooldownMs = clampInt(
      msg.options?.meetingCooldownMs ?? meetingConfig?.cooldownMs,
      0,
      10_000,
      DEFAULT_MEETING_COOLDOWN_MS
    );
    meetingEchoSuppressMs = clampInt(
      msg.options?.echoSuppressMs ?? meetingConfig?.echoSuppressMs,
      0,
      10_000,
      DEFAULT_MEETING_ECHO_SUPPRESS_MS
    );
    meetingEchoSimilarity = clampNumber(
      msg.options?.echoSimilarity ?? meetingConfig?.echoSimilarity,
      0,
      1,
      DEFAULT_MEETING_ECHO_SIMILARITY
    );
    meetingIntroEnabled = meetingConfig?.introEnabled ?? true;
    meetingIntroText =
      meetingConfig?.introText ??
      (lang.toLowerCase().startsWith('ja') ? DEFAULT_MEETING_INTRO_TEXT_JA : DEFAULT_MEETING_INTRO_TEXT_EN);
    meetingOpenUntil = 0;
    meetingCooldownUntil = 0;
    meetingWindowOpen = false;
    meetingIntroSent = false;
    lastAssistantText = null;
    applyMeetingSystemPrompt();

    if (presetMode === 'openai_realtime') {
      if (msg.channelSplit) {
        throw new Error('openai_realtime preset does not support channelSplit; use pipeline presets for meeting mode');
      }
      if (selectedSttProvider !== 'openai' || selectedTtsProvider !== 'openai') {
        throw new Error('openai_realtime preset requires sttProvider=openai and ttsProvider=openai');
      }

      outputSampleRate = openAiRealtimeSampleRate;
      await startOrRestartRealtimeSession();

      const sessionMsg: VoiceSessionMessage = {
        type: 'voice_session',
        sessionId,
        startedAt,
        presetId: presetId ?? undefined,
        mode: presetMode,
        inputSampleRate: clientSampleRate,
        outputAudioSpec: { sampleRate: outputSampleRate, channels: 1, format: 'pcm16le' },
        sttProvider: selectedSttProvider,
        llmProvider,
        ttsProvider: selectedTtsProvider,
      };
      sendJson(sessionMsg);
      sendState('listening');

      if (shouldKeepalive) {
        keepaliveTimer = setInterval(() => {
          if (closed) return;
          if (missedPongs >= maxMissedPongs) {
            handleFatal(new Error('voice keepalive timeout'));
            return;
          }
          missedPongs += 1;
          sendJson({ type: 'ping', ts: Date.now() });
        }, keepaliveMs);
      }

      return;
    }

    // Align pipeline-mode input/output sample rate with the selected STT provider.
    // OpenAI realtime transcription expects 24 kHz mono PCM; others default to config.audio.targetSampleRate.
    outputSampleRate = getProviderSampleRate(selectedSttProvider, config);

    const adapter = getAdapter(selectedSttProvider);
    if (!adapter.supportsStreaming) {
      throw new Error(`voice STT provider does not support streaming: ${selectedSttProvider}`);
    }

    const useChannelSplit = meetingModeEnabled && msg.channelSplit === true && msg.channels === 2;
    const sources: VoiceInputSource[] = useChannelSplit ? ['mic', 'meeting'] : ['mic'];

    const handleSttTranscript = (source: VoiceInputSource, t: { isFinal: boolean; text: string; speakerId?: string }) => {
      if (closed) return;
      const now = Date.now();
      if (source === 'meeting' && meetingModeEnabled) {
        refreshMeetingWindow(now);
      }

      const resolved = buildTranscriptInfo(source, t.text, now);
      if (!resolved) return;
      const { info, wakeWordMatched, cleanedText, openWindowActive } = resolved;

      if (source === 'meeting' && meetingModeEnabled) {
        if (isEchoLikely(info.displayText, cleanedText, now)) {
          logger.info({
            event: 'voice_echo_suppressed',
            sessionId,
            wakeWordMatched,
            openWindowActive,
          });
          return;
        }

        if (meetingRequireWakeWord) {
          if (wakeWordMatched) {
            openMeetingWindow(now, 'wake_word');
          } else if (openWindowActive && info.triggerText) {
            extendMeetingWindow(now);
          }
        }
      }

      // If the user starts talking while the assistant is still thinking, abort the in-flight turn.
      if (assistantTurn?.state === 'thinking' && info.triggerText) {
        stopAssistant('barge_in');
      }

      if (voiceState === 'speaking') {
        if (t.isFinal) {
          suppressedFinalParts.push({ source, info, speakerId: t.speakerId });
        } else {
          suppressedInterim = { source, info, speakerId: t.speakerId };
        }
        if (source === 'meeting' && info.triggerText) {
          stopAssistant('barge_in');
          applySuppressedTranscripts();
        }
        return;
      }

      sendUserTranscript(source, info, t.isFinal, t.speakerId);
      if (t.isFinal && info.triggerText) {
        if (enqueueFinalPart(info.triggerText)) {
          scheduleFinalize();
        }
      }
    };

    await Promise.all(
      sources.map(async (source) => {
        const sttSession = await adapter.startStreaming({
          language: lang,
          sampleRateHz: outputSampleRate,
          encoding: 'linear16',
          enableInterim,
          enableVad: true,
          vad: voiceVad,
        });
        sttControllers.set(source, sttSession.controller);
        sttSession.onData((t) => handleSttTranscript(source, t));
        sttSession.onError((err) => handleFatal(err));
        sttSession.onClose(() => {
          if (closed) return;
          ws.close();
        });
      })
    );

    const sessionMsg: VoiceSessionMessage = {
      type: 'voice_session',
      sessionId,
      startedAt,
      presetId: presetId ?? undefined,
      mode: presetMode,
      inputSampleRate: clientSampleRate,
      outputAudioSpec: { sampleRate: outputSampleRate, channels: 1, format: 'pcm16le' },
      sttProvider: selectedSttProvider,
      llmProvider,
      ttsProvider: selectedTtsProvider,
    };
    sendJson(sessionMsg);
    sendState('listening');

    if (shouldKeepalive) {
      keepaliveTimer = setInterval(() => {
        if (closed) return;
        if (missedPongs >= maxMissedPongs) {
          handleFatal(new Error('voice keepalive timeout'));
          return;
        }
        missedPongs += 1;
        sendJson({ type: 'ping', ts: Date.now() });
      }, keepaliveMs);
    }

    if (meetingModeEnabled && meetingIntroEnabled && !meetingIntroSent) {
      const intro = meetingIntroText?.trim();
      if (intro) {
        meetingIntroSent = true;
        void speakAssistantText(intro);
      }
    }
  };

  const handleCommand = async (msg: VoiceCommandMessage) => {
    if (msg.name === 'reset_history') {
      const hadAssistant = assistantTurn !== null || realtimeActive !== null;
      stopAssistant('stopped', { playedMs: msg.playedMs });
      closeMeetingWindow(Date.now(), 'manual');
      history.length = 0;
      history.push({ role: 'system', content: systemPrompt });
      applyMeetingSystemPrompt();
      pendingFinalParts.length = 0;
      clearSuppressedTranscripts();
      if (presetMode === 'openai_realtime') {
        await startOrRestartRealtimeSession();
      }
      if (!hadAssistant) {
        sendState('listening');
      }
      return;
    }

    if (msg.name === 'stop_speaking') {
      stopAssistant('stopped', { playedMs: msg.playedMs });
      return;
    }

    if (msg.name === 'barge_in') {
      stopAssistant('barge_in', { playedMs: msg.playedMs });
      applySuppressedTranscripts();
    }
  };

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const parsed = JSON.parse(data.toString()) as { type?: string };
        if (parsed?.type === 'pong') {
          missedPongs = 0;
          return;
        }
      } catch {
        // ignore parse errors for control frames
      }
    }

    const run = async () => {
      if (closed) return;
      if (!sessionStarted && isBinary) {
        sendJson({ type: 'error', message: 'config message required before audio' });
        ws.close();
        return;
      }
      if (!sessionStarted && !isBinary) {
        try {
          const parsed = JSON.parse(data.toString());
          const cfg = voiceConfigMessageSchema.parse(parsed) as VoiceConfigMessage;
          sessionStarted = true;
          await startStt(cfg);
          return;
        } catch (err) {
          const message = err instanceof Error ? err.message : 'Invalid initial config message';
          sendJson({ type: 'error', message });
          ws.close();
          return;
        }
      }

      if (!isBinary) {
        try {
          const parsed = JSON.parse(data.toString());
          const cmd = voiceCommandMessageSchema.parse(parsed) as VoiceCommandMessage;
          await handleCommand(cmd);
        } catch {
          // ignore unknown messages
        }
        return;
      }

      const buffer = normalizeRawData(data);
      try {
        const { header, pcm } = parseStreamFrame(buffer);
        const source: VoiceInputSource =
          meetingModeEnabled && sttControllers.has('meeting') ? (header.seq % 2 === 0 ? 'mic' : 'meeting') : 'mic';
        const resample = ensureResampler(source, clientSampleRate);
        if (resample) {
          await resample.input(pcm, {
            captureTs: header.captureTs,
            durationMs: header.durationMs,
            seq: header.seq,
          });
          return;
        }
        await handlePcmChunk(source, pcm, header);
      } catch (err) {
        handleFatal(err as Error);
      }
    };
    if (closed) return;
    messageChain = messageChain.then(run).catch((err) => {
      handleFatal(err as Error);
    });
  });

  ws.on('close', () => {
    closed = true;
    if (keepaliveTimer) {
      clearInterval(keepaliveTimer);
      keepaliveTimer = null;
    }
    if (finalizeTimer) {
      clearTimeout(finalizeTimer);
      finalizeTimer = null;
    }
    stopAssistant('stopped');
    resamplers.forEach((r) => r.end());
    void (async () => {
      await Promise.allSettled(Array.from(sttPendings.values()).map(async (p) => p.catch(() => undefined)));
      await Promise.allSettled(Array.from(sttControllers.values()).map(async (c) => c.end().catch(() => undefined)));
      await Promise.allSettled(Array.from(sttControllers.values()).map(async (c) => c.close().catch(() => undefined)));
      await realtimePending?.catch(() => undefined);
      await realtimeSession?.close().catch(() => undefined);
      realtimeSession = null;
      realtimeActive = null;
    })();
  });

  ws.on('error', (err) => {
    logger.error({ event: 'voice_ws_error', message: err.message });
    handleFatal(err);
  });
}

function normalizeRawData(data: RawData): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  }
  if (data instanceof ArrayBuffer) {
    return Buffer.from(data);
  }
  if (Array.isArray(data)) {
    return Buffer.concat(data);
  }
  return Buffer.from(data as ArrayBuffer);
}
