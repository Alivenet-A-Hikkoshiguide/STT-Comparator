import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { z } from 'zod';
import { PROVIDER_IDS } from './types.js';
import type { ProviderId } from './types.js';

export const FAIR_SAMPLE_RATE_HZ = 16_000;
export const FAIR_CHANNELS = 1;

const normalizationSchema = z.object({
  nfkc: z.boolean().optional(),
  stripPunct: z.boolean().optional(),
  stripSpace: z.boolean().optional(),
  lowercase: z.boolean().optional(),
});

const ingressNormalizeSchema = z
  .object({
    enabled: z.boolean().optional(),
    targetSampleRate: z
      .number()
      .int()
      .refine((value) => value === FAIR_SAMPLE_RATE_HZ, {
        message: `ingressNormalize.targetSampleRate must be ${FAIR_SAMPLE_RATE_HZ}`,
      })
      .optional(),
    targetChannels: z
      .number()
      .int()
      .refine((value) => value === FAIR_CHANNELS, {
        message: `ingressNormalize.targetChannels must be ${FAIR_CHANNELS}`,
      })
      .optional(),
    peakDbfs: z.number().optional(),
    maxDurationSec: z.number().min(0.1).max(3600).optional(),
  })
  .partial()
  .default({});

const voicePresetSchema = z.object({
  id: z.string().min(1).max(50),
  label: z.string().min(1).max(100).optional(),
  mode: z.enum(['pipeline', 'openai_realtime']).optional(),
  sttProvider: z.enum(PROVIDER_IDS),
  ttsProvider: z.enum(PROVIDER_IDS),
});

const voiceVadSchema = z
  .object({
    threshold: z.number().min(0).max(1).optional(),
    silenceDurationMs: z.number().int().min(50).max(5000).optional(),
    prefixPaddingMs: z.number().int().min(0).max(2000).optional(),
  })
  .partial()
  .default({});

const meetingGateVadSchema = z
  .object({
    enabled: z.boolean().optional(),
    mode: z.number().int().min(0).max(3).optional(),
    frameMs: z.number().int().min(10).max(30).optional(),
    minSpeechFrames: z.number().int().min(1).max(10).optional(),
    speechRatio: z.number().min(0.1).max(1).optional(),
  })
  .partial()
  .default({});

const voiceMeetingGateSchema = z
  .object({
    enabled: z.boolean().optional(),
    minRms: z.number().min(0).max(1).optional(),
    noiseAlpha: z.number().min(0).max(1).optional(),
    openFactor: z.number().min(1).max(20).optional(),
    closeFactor: z.number().min(1).max(20).optional(),
    hangoverMs: z.number().int().min(0).max(5000).optional(),
    assistantGuardFactor: z.number().min(1).max(5).optional(),
    vad: meetingGateVadSchema.optional(),
  })
  .partial()
  .default({});

const voiceMeetingSchema = z
  .object({
    openWindowMs: z.number().int().min(0).max(30_000).optional(),
    cooldownMs: z.number().int().min(0).max(10_000).optional(),
    echoSuppressMs: z.number().int().min(0).max(10_000).optional(),
    echoSimilarity: z.number().min(0).max(1).optional(),
    introEnabled: z.boolean().optional(),
    introText: z.string().min(1).max(500).optional(),
  })
  .partial()
  .default({});

const voiceTtsSchema = z
  .object({
    strategy: z.enum(['stream', 'segment-buffered']).optional(),
    segmentMinChars: z.number().int().min(10).max(400).optional(),
    segmentMaxChars: z.number().int().min(20).max(800).optional(),
    retryMax: z.number().int().min(0).max(5).optional(),
    retryBaseMs: z.number().int().min(50).max(5000).optional(),
  })
  .partial()
  .default({});

const voiceSchema = z
  .object({
    presets: z.array(voicePresetSchema).min(1).max(20).optional(),
    defaultPresetId: z.string().min(1).max(50).optional(),
    memoryPath: z.string().min(1).max(500).optional(),
    vad: voiceVadSchema.optional(),
    meetingGate: voiceMeetingGateSchema.optional(),
    meeting: voiceMeetingSchema.optional(),
    tts: voiceTtsSchema.optional(),
  })
  .partial()
  .default({});

export const configSchema = z.object({
  audio: z.object({
    targetSampleRate: z.literal(FAIR_SAMPLE_RATE_HZ).default(FAIR_SAMPLE_RATE_HZ),
    targetChannels: z.literal(FAIR_CHANNELS).default(FAIR_CHANNELS),
    chunkMs: z.number().default(250),
  }),
  ingressNormalize: ingressNormalizeSchema.optional(),
  normalization: normalizationSchema,
  storage: z.object({
    driver: z.enum(['jsonl', 'sqlite']),
    path: z.string().default('./runs/latest'),
    retentionDays: z.number().min(1).max(3650).default(30),
    maxRows: z.number().min(100).max(1_000_000).default(100_000),
  }),
  providers: z.array(z.enum(PROVIDER_IDS)),
  voice: voiceSchema.optional(),
  jobs: z
    .object({
      maxParallel: z.number().min(1).max(64).default(4),
      retentionMs: z.number().min(0).default(10 * 60 * 1000),
      persistenceMode: z.literal('required').default('required'),
      stateDbPath: z
        .string()
        .min(1)
        .refine((value) => !value.includes('{date}'), {
          message: 'jobs.stateDbPath must not include {date}',
        })
        .default('./runs/job-state/batch-jobs.sqlite'),
      providerMaxParallel: z.record(z.enum(PROVIDER_IDS), z.number().int().min(1).max(64)).optional(),
      retry: z
        .object({
          maxAttempts: z.number().int().min(1).max(8).default(3),
          baseDelayMs: z.number().int().min(100).max(30_000).default(1000),
          maxDelayMs: z.number().int().min(100).max(60_000).default(10_000),
        })
        .default({}),
    })
    .default({}),
  ws: z
    .object({
      maxPcmQueueBytes: z.number().min(128 * 1024).max(100 * 1024 * 1024).default(5 * 1024 * 1024),
      overflowGraceMs: z.number().int().min(100).max(30_000).default(500),
      keepaliveMs: z.number().int().min(5_000).max(120_000).default(30_000),
      maxMissedPongs: z.number().int().min(1).max(10).default(2),
      meeting: z
        .object({
          maxPcmQueueBytes: z.number().int().min(128 * 1024).max(100 * 1024 * 1024).optional(),
          overflowGraceMs: z.number().int().min(100).max(10_000).optional(),
        })
        .partial()
        .default({}),
      preview: z
        .object({
          maxUploadBytes: z.number().int().min(1 * 1024 * 1024).max(1024 * 1024 * 1024).optional(),
        })
        .partial()
        .default({}),
      replay: z
        .object({
          minDurationMs: z.number().int().min(0).max(10_000).optional(),
          maxUploadBytes: z.number().int().min(1 * 1024 * 1024).max(1024 * 1024 * 1024).optional(),
        })
        .partial()
        .default({}),
      compare: z
        .object({
          backlogSoft: z.number().int().min(1).max(10_000).default(8),
          backlogHard: z.number().int().min(2).max(20_000).default(32),
          maxDropMs: z.number().int().min(0).max(60_000).default(1000),
        })
        .partial()
        .default({}),
    })
    .partial()
    .default({}),
  providerHealth: z
    .object({
      refreshMs: z.number().int().min(1).optional(),
      failureThreshold: z.number().int().min(1).max(20).optional(),
      cooldownMs: z.number().int().min(1_000).max(10 * 60_000).optional(),
    })
    .default({}),
  providerLimits: z
    .object({
      batchMaxBytes: z.record(z.enum(PROVIDER_IDS), z.number().int().positive()).optional(),
    })
    .partial()
    .default({}),
});

export type AppConfig = z.infer<typeof configSchema>;

let cachedConfig: AppConfig | null = null;

function formatLocalDate(): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function expandStoragePath(input: string): string {
  if (!input.includes('{date}')) return input;
  return input.replaceAll('{date}', formatLocalDate());
}

export async function loadConfig(configPath = path.resolve('config.json')): Promise<AppConfig> {
  if (cachedConfig) {
    return cachedConfig;
  }

  const raw = await readFile(configPath, 'utf-8');
  const parsed = configSchema.parse(JSON.parse(raw));
  const typed: AppConfig = {
    ...parsed,
    storage: {
      ...parsed.storage,
      path: expandStoragePath(parsed.storage.path),
    },
    providers: parsed.providers as ProviderId[],
  };
  cachedConfig = typed;
  return typed;
}

export function reloadConfig(): void {
  cachedConfig = null;
}
