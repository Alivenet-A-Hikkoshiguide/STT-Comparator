export interface TranscriptRow {
  id: string;
  text: string;
  provider: string;
  channel: 'mic' | 'file';
  isFinal: boolean;
  timestamp: number;
  latencyMs?: number;
  speakerId?: string;
  degraded?: boolean;
}

export interface NormalizedRow {
  normalizedId?: string;
  segmentId?: number;
  windowId: number;
  windowStartMs: number;
  windowEndMs: number;
  provider: string;
  textRaw: string;
  textNorm: string;
  textDelta?: string;
  isFinal: boolean;
  revision: number;
  latencyMs?: number;
  originCaptureTs?: number;
  confidence?: number | null;
  punctuationApplied?: boolean | null;
  casingApplied?: boolean | null;
  words?: TranscriptWord[];
}

export interface TranscriptWord {
  startSec: number;
  endSec: number;
  text: string;
  confidence?: number;
}

export interface JobStatus {
  jobId: string;
  total: number;
  done: number;
  failed: number;
  warnings?: JobWarning[];
}

export interface SummaryStats {
  n: number;
  avg: number | null;
  p50: number | null;
  p95: number | null;
}

export interface DegradedSummary {
  count: number;
  ratio: number;
}

export interface JobWarning {
  code: 'parallel_clamped' | 'degraded_audio_detected';
  message: string;
  createdAt: string;
  details?: Record<string, unknown>;
}

export interface JobSummary {
  count: number;
  cer: SummaryStats;
  wer: SummaryStats;
  rtf: SummaryStats;
  latencyMs: SummaryStats;
  degraded: DegradedSummary;
}

export interface NormalizationConfig {
  nfkc?: boolean;
  stripPunct?: boolean;
  stripSpace?: boolean;
  lowercase?: boolean;
}

export interface JobHistoryEntry {
  jobId: string;
  provider: string;
  providers?: string[];
  lang: string;
  createdAt: string;
  updatedAt: string;
  total: number;
  done?: number;
  failed?: number;
  status?: 'queued' | 'running' | 'completed' | 'failed';
  resultCount?: number;
  errors?: Array<{ file: string; message: string }>;
  warnings?: JobWarning[];
  summary: JobSummary;
  summaryByProvider?: Record<string, JobSummary>;
}

export interface ProviderInfo {
  id: string;
  available: boolean;
  reason?: string;
  implemented?: boolean;
  supportsStreaming?: boolean;
  supportsBatch?: boolean;
  supportsDictionaryPhrases?: boolean;
  supportsPunctuationPolicy?: boolean;
  supportsContextPhrases?: boolean;
  supportsDiarization?: boolean;
}

export interface FileResult {
  path: string;
  provider: string;
  cer?: number | null;
  wer?: number | null;
  rtf?: number | null;
  latencyMs?: number | null;
  durationSec?: number | null;
  degraded?: boolean;
  text?: string;
  normalizationUsed?: NormalizationConfig;
}

export interface RealtimeLatencySummary {
  sessionId: string;
  provider: string;
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

export type RealtimeLogPayloadType = 'session' | 'transcript' | 'normalized' | 'error' | 'session_end';

export interface RealtimeLogPayloadSession {
  type: 'session';
  sessionId: string;
  provider: string;
  startedAt: string;
  inputSampleRate?: number;
  audioSpec?: { sampleRate: number; channels: number; format: string };
}

export interface RealtimeLogPayloadTranscript {
  type: 'transcript';
  provider?: string;
  isFinal: boolean;
  text: string;
  timestamp: number;
  channel: 'mic' | 'file';
  latencyMs?: number;
  speakerId?: string;
  words?: TranscriptWord[];
  confidence?: number;
}

export interface RealtimeLogPayloadError {
  type: 'error';
  message: string;
}

export interface RealtimeLogPayloadSessionEnd {
  type: 'session_end';
  endedAt: string;
  reason?: string;
}

export type RealtimeLogPayload =
  | RealtimeLogPayloadSession
  | RealtimeLogPayloadTranscript
  | RealtimeLogPayloadError
  | RealtimeLogPayloadSessionEnd
  | (NormalizedRow & { type: 'normalized' });

export interface RealtimeLogEntry {
  sessionId: string;
  provider: string;
  lang: string;
  recordedAt: string;
  payload: RealtimeLogPayload;
}

export interface RealtimeLogSession {
  sessionId: string;
  provider: string;
  lang: string;
  startedAt: string | null;
  lastRecordedAt: string;
  entryCount: number;
}

export interface WsPayload {
  type: 'session' | 'transcript' | 'normalized' | 'error' | 'ping';
  sessionId?: string;
  latencyMs?: number;
  message?: string;
  text?: string;
  provider?: string;
  isFinal?: boolean;
  timestamp?: number;
  degraded?: boolean;
  normalizedId?: string;
  segmentId?: number;
  windowId?: number;
  windowStartMs?: number;
  windowEndMs?: number;
  textRaw?: string;
  textNorm?: string;
  textDelta?: string;
  revision?: number;
  originCaptureTs?: number;
  confidence?: number | null;
  punctuationApplied?: boolean | null;
  casingApplied?: boolean | null;
  words?: TranscriptWord[];
  channel?: 'mic' | 'file';
  speakerId?: string;
  ts?: number;
}

export type PunctuationPolicy = 'none' | 'basic' | 'full';

export interface SubmitBatchInput {
  files: File[] | null;
  manifestJson: string;
  providers: string[];
  lang: string;
  dictionaryPhrases: string[];
  enableVad: boolean;
  punctuationPolicy: PunctuationPolicy;
  parallel: number;
}
