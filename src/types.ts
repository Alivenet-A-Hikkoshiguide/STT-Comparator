export const PROVIDER_IDS = [
  'google',
  'aws',
  'azure',
  'deepgram',
  'elevenlabs',
  'revai',
  'speechmatics',
  'openai',
  'local_whisper',
  'nvidia_riva',
  'whisper_streaming',
  'mock',
] as const;

export type ProviderId = (typeof PROVIDER_IDS)[number];

export interface EffectiveAudioSpec {
  sampleRate: number;
  channels: 1;
  format: 'pcm16le';
}

export type VadConfig = {
  threshold?: number;
  silenceDurationMs?: number;
  prefixPaddingMs?: number;
};

export interface StreamingOptions {
  language: string;
  sampleRateHz: number;
  encoding: 'linear16';
  enableInterim?: boolean;
  enableDiarization?: boolean;
  /** Optional provider-specific model override; falls back to adapter defaults. */
  model?: string;
  /** Optional batch-only model override; falls back to model/defaults. */
  batchModel?: string;
  /** Optional fallback model used when primary fails (batch). */
  fallbackModel?: string;
  contextPhrases?: readonly string[];
  punctuationPolicy?: PunctuationPolicy;
  enableVad?: boolean;
  /** Optional provider VAD tuning (when provider supports server VAD). */
  vad?: VadConfig;
  dictionaryPhrases?: readonly string[];
  normalizePreset?: string;
  parallel?: number;
}

export interface TranscriptWord {
  startSec: number;
  endSec: number;
  text: string;
  confidence?: number;
  speakerId?: string;
}

export interface PartialTranscript {
  provider: ProviderId;
  isFinal: boolean;
  text: string;
  words?: readonly TranscriptWord[];
  confidence?: number;
  /** true if provider already inserted punctuation */
  punctuationApplied?: boolean;
  /** true if provider preserved original casing */
  casingApplied?: boolean;
  /** Optional speaker/diarization label provided by the ASR backend. */
  speakerId?: string;
  timestamp: number;
  channel: 'mic' | 'file';
  latencyMs?: number;
  /** Capture timestamp (ms, wall clock) of the audio chunk that produced this transcript, if known. */
  originCaptureTs?: number;
  /** Optional sequence number to correlate audio->transcript when providers keep order. */
  seq?: number;
  degraded?: boolean;
}

export type PunctuationPolicy = 'none' | 'basic' | 'full';

export interface TranscriptionOptions {
  enableVad?: boolean;
  enableDiarization?: boolean;
  enableChannelSplit?: boolean;
  meetingMode?: boolean;
  punctuationPolicy?: PunctuationPolicy;
  dictionaryPhrases?: readonly string[];
  parallel?: number;
}

export interface StreamingConfigMessage {
  type: 'config';
  enableInterim?: boolean;
  contextPhrases?: readonly string[];
  normalizePreset?: string;
  pcm?: boolean;
  degraded?: boolean;
  /** Actual AudioContext sample rate observed on the client. */
  clientSampleRate?: number;
  /** Number of channels captured by the client (1=mono, 2=stereo). */
  channels?: number;
  /** Enable client-side L/R channel split when sending PCM. */
  channelSplit?: boolean;
  options?: TranscriptionOptions;
}

export interface StreamingController {
  sendAudio(chunk: ArrayBufferLike, meta?: { captureTs?: number; seq?: number }): Promise<void>;
  end(): Promise<void>;
  close(): Promise<void>;
}

export interface StreamingSession {
  controller: StreamingController;
  onData(cb: (t: PartialTranscript) => void): void;
  onError(cb: (err: Error) => void): void;
  onClose(cb: () => void): void;
}

export interface BatchResult {
  provider: ProviderId;
  text: string;
  words?: readonly TranscriptWord[];
  durationSec?: number;
  /**
   * Processing time reported by the provider (if available).
   * Used for observability only; latencyMs is computed server-side for fairness.
   */
  vendorProcessingMs?: number;
}

export interface ProviderAdapter {
  id: ProviderId;
  supportsStreaming: boolean;
  supportsBatch: boolean;
  startStreaming(opts: StreamingOptions): Promise<StreamingSession>;
  transcribePcmBuffer?(pcm: Buffer, opts: StreamingOptions): Promise<BatchResult>;
  transcribeFileFromPCM(
    pcm: NodeJS.ReadableStream,
    opts: StreamingOptions
  ): Promise<BatchResult>;
}

export interface StreamTranscriptMessage extends PartialTranscript {
  type: 'transcript';
}

export interface NormalizedTranscriptMessage {
  type: 'normalized';
  provider: ProviderId;
  normalizedId: string;
  segmentId: number;
  windowId: number;
  windowStartMs: number;
  windowEndMs: number;
  textRaw: string;
  textNorm: string;
  /** Portion of text newly added since the previous revision for this provider (optional). */
  textDelta?: string;
  isFinal: boolean;
  revision: number;
  latencyMs?: number;
  originCaptureTs?: number;
  speakerId?: string;
  confidence?: number | null;
  punctuationApplied?: boolean | null;
  casingApplied?: boolean | null;
  words?: PartialTranscript['words'];
}

export interface StreamErrorMessage {
  type: 'error';
  message: string;
  provider?: ProviderId;
}

export interface StreamSessionMessage {
  type: 'session';
  sessionId: string;
  provider: ProviderId;
  startedAt: string;
  /** Observed input spec from client/replay source before per-provider resampling. */
  inputSampleRate?: number;
  /** Effective output spec delivered to the provider after resampling. */
  audioSpec?: EffectiveAudioSpec;
}

export type StreamServerMessage =
  | StreamTranscriptMessage
  | NormalizedTranscriptMessage
  | StreamErrorMessage
  | StreamSessionMessage;

export interface StreamSessionEndMessage {
  type: 'session_end';
  endedAt: string;
  reason?: string;
}

export type RealtimeLogPayload = StreamServerMessage | StreamSessionEndMessage;

export interface RealtimeTranscriptLogEntry {
  sessionId: string;
  provider: ProviderId;
  lang: string;
  recordedAt: string;
  payload: RealtimeLogPayload;
}

export interface RealtimeTranscriptSessionSummary {
  sessionId: string;
  provider: ProviderId;
  lang: string;
  startedAt: string | null;
  lastRecordedAt: string;
  entryCount: number;
}

export type VoiceState = 'listening' | 'thinking' | 'speaking';

export type VoiceAgentMode = 'pipeline' | 'openai_realtime';

export type VoiceInputSource = 'mic' | 'meeting';

export interface VoiceConfigMessage {
  type: 'config';
  pcm: true;
  clientSampleRate: number;
  enableInterim?: boolean;
  /** Optional voice preset id selected by the client. */
  presetId?: string;
  /** Number of channels captured by the client (1=mono, 2=stereo). */
  channels?: number;
  /** Enable client-side L/R channel split when sending PCM. */
  channelSplit?: boolean;
  options?: {
    /** Silence window to finalize a user turn. */
    finalizeDelayMs?: number;
    /** Enables web-meeting optimized behavior (e.g., wake-word gating). */
    meetingMode?: boolean;
    /** Require wake words for meeting audio to trigger replies. */
    meetingRequireWakeWord?: boolean;
    /** Whether assistant audio is routed into the meeting output (affects gating). */
    meetingOutputEnabled?: boolean;
    /** Wake words used when meetingRequireWakeWord is enabled. */
    wakeWords?: readonly string[];
    /** Open-window duration (ms) after a wake word. */
    meetingOpenWindowMs?: number;
    /** Cooldown between open-window activations (ms). */
    meetingCooldownMs?: number;
    /** Echo suppression window after assistant speech (ms). */
    echoSuppressMs?: number;
    /** Similarity threshold for echo suppression (0..1). */
    echoSimilarity?: number;
  };
}

export interface VoiceCommandMessage {
  type: 'command';
  name: 'barge_in' | 'stop_speaking' | 'reset_history';
  /**
   * Best-effort estimate of how much assistant audio has actually been played to the user (ms).
   * Used to truncate the remote conversation audio on interruptions.
   */
  playedMs?: number;
}

export interface VoiceStateMessage {
  type: 'voice_state';
  state: VoiceState;
  ts: number;
  turnId?: string;
}

export interface VoiceSessionMessage {
  type: 'voice_session';
  sessionId: string;
  startedAt: string;
  presetId?: string;
  mode?: VoiceAgentMode;
  inputSampleRate: number;
  outputAudioSpec: EffectiveAudioSpec;
  sttProvider: ProviderId;
  llmProvider: 'openai';
  ttsProvider: ProviderId;
}

export interface VoiceUserTranscriptMessage {
  type: 'voice_user_transcript';
  isFinal: boolean;
  text: string;
  timestamp: number;
  source?: VoiceInputSource;
  speakerId?: string;
  /** Whether this transcript can trigger assistant responses (e.g., wake word matched). */
  triggered?: boolean;
}

export type UrlCitation = {
  url: string;
  title?: string;
  startIndex: number;
  endIndex: number;
};

export interface VoiceAssistantTextMessage {
  type: 'voice_assistant_text';
  turnId: string;
  text: string;
  citations?: UrlCitation[];
  isFinal: boolean;
  timestamp: number;
}

export interface VoiceAssistantAudioStartMessage {
  type: 'voice_assistant_audio_start';
  turnId: string;
  timestamp: number;
  llmMs?: number;
  ttsTtfbMs?: number;
}

export interface VoiceAssistantAudioEndMessage {
  type: 'voice_assistant_audio_end';
  turnId: string;
  timestamp: number;
  reason?: 'completed' | 'barge_in' | 'stopped' | 'error';
}

export interface VoiceMeetingWindowMessage {
  type: 'voice_meeting_window';
  state: 'opened' | 'closed';
  ts: number;
  expiresAt?: number;
  reason?: 'wake_word' | 'timeout' | 'manual' | 'cooldown';
}

export type VoiceServerMessage =
  | VoiceSessionMessage
  | VoiceStateMessage
  | VoiceUserTranscriptMessage
  | VoiceAssistantTextMessage
  | VoiceAssistantAudioStartMessage
  | VoiceAssistantAudioEndMessage
  | VoiceMeetingWindowMessage
  | StreamErrorMessage;

export interface NormalizationConfig {
  nfkc?: boolean;
  stripPunct?: boolean;
  stripSpace?: boolean;
  lowercase?: boolean;
}

export interface ManifestItem {
  audio: string;
  ref: string;
  meta?: Record<string, unknown>;
}

export interface EvaluationManifest {
  version: number;
  language: string;
  items: ManifestItem[];
  normalization?: NormalizationConfig;
  allowBasenameFallback?: boolean;
}

export interface StorageDriver<T> {
  init(): Promise<void>;
  append(record: T): Promise<void>;
  readAll(): Promise<T[]>;
  readRecent?(limit: number): Promise<T[]>;
  readByJob?(jobId: string): Promise<T[]>;
  listJobIds?(): Promise<string[]>;
}

export interface BatchAudioSpec {
  sampleRateHz: number;
  channels: number;
  encoding: 'linear16';
}

export interface BatchJobFileResult {
  jobId: string;
  path: string;
  provider: ProviderId;
  lang: string;
  durationSec: number;
  /** Server-measured wall clock processing time (adapter invocation -> return). */
  processingTimeMs: number;
  rtf: number;
  cer?: number;
  wer?: number;
  /**
   * Batch latency in milliseconds (server-measured wall-clock processing time for the adapter call).
   * Realtime streaming latency is computed per transcript message and stored separately.
   */
  latencyMs?: number;
  text: string;
  refText?: string;
  degraded?: boolean;
  opts?: Record<string, unknown>;
  /** Normalization actually applied when scoring */
  normalizationUsed?: NormalizationConfig;
  /** ISO timestamp when the record was created (for retention / pruning) */
  createdAt?: string;
  /** Provider-reported processing time (if supplied by adapter) */
  vendorProcessingMs?: number;
  /** Audio spec actually passed to providers for fairness/audit */
  audioSpec?: BatchAudioSpec;
}

export interface JobWarning {
  code: 'parallel_clamped' | 'degraded_audio_detected';
  message: string;
  createdAt: string;
  details?: Record<string, unknown>;
}

export interface DegradedSummary {
  count: number;
  ratio: number;
}

export type StorageDriverName = 'jsonl' | 'sqlite';

export interface SummaryStats {
  /** Number of valid samples included in this metric (finite values only). */
  n: number;
  avg: number | null;
  p50: number | null;
  p95: number | null;
}

export interface JobSummary {
  count: number;
  cer: SummaryStats;
  wer: SummaryStats;
  rtf: SummaryStats;
  latencyMs: SummaryStats;
  degraded: DegradedSummary;
}

export interface RealtimeLatencySummary {
  sessionId: string;
  provider: ProviderId;
  lang: string;
  count: number;
  avg: number | null;
  p50: number | null;
  p95: number | null;
  min: number | null;
  max: number | null;
  startedAt: string;
  endedAt: string;
}
