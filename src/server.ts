import { createServer } from 'node:http';
import path from 'node:path';
import { existsSync, createReadStream } from 'node:fs';
import { unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import multer from 'multer';
import { WebSocket, WebSocketServer } from 'ws';
import { handleStreamConnection } from './ws/streamHandler.js';
import { handleCompareStreamConnection } from './ws/compareStreamHandler.js';
import { handleReplayConnection, handleReplayMultiConnection } from './ws/replayHandler.js';
import { handleVoiceConnection } from './ws/voiceHandler.js';
import { getVoicePresetCatalog } from './voice/voicePresets.js';
import { logger } from './logger.js';
import { createRealtimeStorage, createRealtimeTranscriptStore, createStorage } from './storage/index.js';
import { loadConfig, reloadConfig } from './config.js';
import { loadEnvironment, reloadEnvironment } from './utils/env.js';
import { BatchRunner } from './jobs/batchRunner.js';
import { JobExportService } from './jobs/jobExportService.js';
import { JobHistory } from './jobs/jobHistory.js';
import { PersistentBatchJobStore } from './jobs/persistentBatchJobStore.js';
import { parseManifest, validateManifestCoverage } from './utils/manifest.js';
import { toCsvStream } from './storage/csvExporter.js';
import { summarizeJob, summarizeJobByProvider } from './utils/summary.js';
import { requireProviderAvailable } from './utils/providerStatus.js';
import { ProviderAvailabilityCache } from './utils/providerAvailabilityCache.js';
import { getAdapter } from './adapters/index.js';
import { transcriptionOptionsSchema } from './validation.js';
import { PreviewStore } from './replay/previewStore.js';
import { AudioDecodeError } from './utils/audioNormalizer.js';
import { ensureNormalizedAudio, AudioValidationError } from './utils/audioIngress.js';
import { findInvalidAudioUploads, getAllowedAudioExtensions } from './utils/audioUploadValidation.js';
import { assertFfmpegAvailable } from './utils/ffmpeg.js';
import type {
  ProviderId,
  RealtimeLatencySummary,
  StorageDriver,
  TranscriptionOptions,
  EvaluationManifest,
} from './types.js';
import type { AppConfig } from './config.js';
import type { ProviderAvailability } from './utils/providerStatus.js';
import { ReplaySessionStore } from './replay/replaySessionStore.js';
import type { RealtimeTranscriptLogStore } from './storage/realtimeTranscriptStore.js';
import { getBatchMaxBytes } from './utils/providerLimits.js';

loadEnvironment();

export class HttpError extends Error {
  statusCode: number;
  payload?: Record<string, unknown>;

  constructor(statusCode: number, message: string, payload?: Record<string, unknown>) {
    super(message);
    this.statusCode = statusCode;
    this.payload = payload;
  }
}

interface StatusCodeError extends Error {
  statusCode?: number;
}

interface ParsedBatchRequest {
  files: Express.Multer.File[];
  providers: ProviderId[];
  provider?: ProviderId;
  lang: string;
  manifest?: EvaluationManifest;
  options?: TranscriptionOptions;
}

function normalizeLanguageTag(value: string): string {
  return value.trim().toLowerCase();
}

function isLanguageCompatible(requestedLang: string, manifestLang: string): boolean {
  const requested = normalizeLanguageTag(requestedLang);
  const manifest = normalizeLanguageTag(manifestLang);
  if (!requested || !manifest) {
    return false;
  }
  if (requested === manifest) {
    return true;
  }
  const requestedPrimary = requested.split('-')[0] ?? '';
  const manifestPrimary = manifest.split('-')[0] ?? '';
  return requestedPrimary.length > 0 && requestedPrimary === manifestPrimary;
}

export function parseBatchRequest(
  req: express.Request,
  providerAvailability: ProviderAvailability[],
  config: AppConfig
): ParsedBatchRequest {
  const files = (req.files as Express.Multer.File[]) ?? [];
  const provider = req.body.provider as ProviderId | undefined;
  const providersRaw = req.body.providers as string | string[] | undefined;
  const langRaw = req.body.lang as string | undefined;
  if (!provider && !providersRaw) {
    throw new HttpError(400, 'provider or providers is required');
  }
  if (!langRaw || langRaw.trim().length === 0) {
    throw new HttpError(400, 'lang is required');
  }
  const requestedLang = langRaw.trim();
  if (files.length === 0) {
    throw new HttpError(400, 'no files uploaded');
  }
  const invalidFiles = findInvalidAudioUploads(files);
  if (invalidFiles.length > 0) {
    throw new HttpError(
      400,
      'unsupported file type detected; upload audio files only',
      {
        invalidFiles,
        allowedExtensions: getAllowedAudioExtensions(),
      }
    );
  }

  const providers = (() => {
    if (providersRaw) {
      try {
        const parsed = Array.isArray(providersRaw)
          ? providersRaw
          : (() => {
              const raw = providersRaw as string;
              if (raw.trim().startsWith('[')) {
                return JSON.parse(raw) as unknown;
              }
              return raw.split(',').map((v) => v.trim()).filter(Boolean);
            })();
        if (!Array.isArray(parsed) || parsed.length === 0) {
          throw new Error('providers must be a non-empty array');
        }
        return Array.from(new Set(parsed)) as ProviderId[];
      } catch (err) {
        throw new HttpError(400, `invalid providers: ${(err as Error).message}`);
      }
    }
    return [provider as ProviderId];
  })();

  if (providers.length === 0) {
    throw new HttpError(400, 'providers must not be empty');
  }

  try {
    providers.forEach((p) => requireProviderAvailable(providerAvailability, p, 'batch'));
  } catch (err) {
    const statusCode = (err as StatusCodeError).statusCode ?? 400;
    const message = err instanceof Error ? err.message : 'provider validation failed';
    throw new HttpError(statusCode, message, { providers: providerAvailability });
  }

  let manifest: EvaluationManifest | undefined;
  if (req.body.ref_json) {
    try {
      manifest = parseManifest(req.body.ref_json as string);
    } catch (err) {
      throw new HttpError(400, `invalid ref_json: ${(err as Error).message}`);
    }
    if (!manifest.language || manifest.language.trim().length === 0) {
      throw new HttpError(400, 'manifest.language is required');
    }
    if (!isLanguageCompatible(requestedLang, manifest.language)) {
      throw new HttpError(
        400,
        `lang mismatch: request lang "${requestedLang}" must match manifest.language "${manifest.language}"`
      );
    }
    const coverage = validateManifestCoverage(
      manifest,
      files.map((file) => file.originalname)
    );
    if (coverage.missingFiles.length > 0 || coverage.ambiguousFiles.length > 0) {
      throw new HttpError(
        400,
        'manifest items could not be matched to uploaded files',
        {
          missingFiles: coverage.missingFiles,
          ambiguousFiles: coverage.ambiguousFiles,
        }
      );
    }
  }

  let options: TranscriptionOptions | undefined;
  if (req.body.options) {
    try {
      const parsed = JSON.parse(req.body.options as string);
      options = transcriptionOptionsSchema.parse(parsed) as TranscriptionOptions;
    } catch (err) {
      throw new HttpError(400, `invalid options: ${(err as Error).message}`);
    }
  }

  for (const p of providers) {
    const maxBytes = getBatchMaxBytes(p, config);
    if (maxBytes) {
      const tooLarge = files.find((f) => f.size && f.size > maxBytes);
      if (tooLarge) {
        throw new HttpError(
          413,
          `file too large for provider ${p} (${Math.round(maxBytes / (1024 * 1024))}MB limit)`
        );
      }
    }
  }

  const effectiveLang = manifest?.language?.trim() || requestedLang;
  return { files, providers, provider: providers[0], lang: effectiveLang, manifest, options };
}

export function createRealtimeLatencyHandler(
  realtimeLatencyStore: StorageDriver<RealtimeLatencySummary>
) {
  return async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    try {
      const raw = Number(req.query.limit ?? 20);
      const limit = Number.isFinite(raw) ? Math.min(50, Math.max(1, raw)) : 20;
      const items =
        typeof realtimeLatencyStore.readRecent === 'function'
          ? await realtimeLatencyStore.readRecent(limit)
          : (await realtimeLatencyStore.readAll()).slice(-limit).reverse();
      res.json(items);
    } catch (error) {
      next(error as Error);
    }
  };
}

export function createRealtimeLogSessionsHandler(store: RealtimeTranscriptLogStore) {
  return async (req: express.Request, res: express.Response, next: express.NextFunction) => {
    try {
      const raw = Number(req.query.limit ?? 20);
      const limit = Number.isFinite(raw) ? Math.min(50, Math.max(1, raw)) : 20;
      const items = await store.listSessions(limit);
      res.json(items);
    } catch (error) {
      next(error as Error);
    }
  };
}

function parseAllowedOrigins(): string[] {
  const raw = process.env.ALLOWED_ORIGINS;
  const defaults = ['http://localhost:4100', 'http://localhost:5173'];
  const list = raw
    ? raw
        .split(',')
        .map((v) => v.trim())
        .filter(Boolean)
    : defaults;
  // de-duplicate
  return Array.from(new Set(list));
}

const toWsOrigin = (origin: string) => origin.replace(/^http(s?):/, 'ws$1:');

function isOriginAllowed(origin: string | undefined, allowed: string[]): boolean {
  if (!origin) return true; // allow same-host tools like curl
  return allowed.includes(origin);
}

function resolveJobStateDbPath(config: AppConfig): string {
  return path.resolve(config.jobs.stateDbPath);
}

export async function cleanupUploadedFiles(files: Express.Multer.File[] | undefined): Promise<void> {
  if (!files?.length) return;
  await Promise.all(
    files
      .map((file) => file.path)
      .filter((filePath): filePath is string => Boolean(filePath))
      .map((filePath) => unlink(filePath).catch(() => undefined))
  );
}

async function bootstrap() {
  const app = express();
  const upload = multer({
    storage: multer.diskStorage({ destination: tmpdir() }),
    limits: { fileSize: 120 * 1024 * 1024 },
  });
  const server = createServer(app);
  const streamWss = new WebSocketServer({ noServer: true });

  await assertFfmpegAvailable();
  let config = await loadConfig();
  const providerHealthRefreshMs = config.providerHealth?.refreshMs ?? 5_000;
  const providerStatusCache = new ProviderAvailabilityCache(config, providerHealthRefreshMs);
  const initialProviders = await providerStatusCache.get();
  const retention = {
    retentionMs: config.storage.retentionDays
      ? config.storage.retentionDays * 24 * 60 * 60 * 1000
      : undefined,
    maxRows: config.storage.maxRows,
  } as const;
  const storage = createStorage(config.storage.driver, config.storage.path, retention);
  const realtimeLatencyStore = createRealtimeStorage(config.storage.driver, config.storage.path, retention);
  const realtimeTranscriptLogStore = createRealtimeTranscriptStore(
    config.storage.driver,
    config.storage.path,
    retention
  );
  const previewStore = new PreviewStore();
  const replaySessionStore = new ReplaySessionStore();
  const jobHistory = new JobHistory(storage);
  await jobHistory.init();
  const jobExporter = new JobExportService(config.storage.path);
  const jobStateDbPath = resolveJobStateDbPath(config);
  const batchJobStore = new PersistentBatchJobStore(jobStateDbPath);
  const batchRunner = new BatchRunner(
    storage,
    jobHistory,
    jobExporter,
    batchJobStore,
    providerStatusCache
  );
  await batchRunner.init();
  await realtimeLatencyStore.init();
  await realtimeTranscriptLogStore.init();
  logger.info({ event: 'providers_status', providers: initialProviders });

  const allowedOrigins = parseAllowedOrigins();
  app.use(
    cors({
      origin: (origin, callback) => {
        if (isOriginAllowed(origin ?? undefined, allowedOrigins)) {
          callback(null, true);
          return;
        }
        callback(new Error('Not allowed by CORS'));
      },
    })
  );
  app.use(express.json({ limit: '2mb' }));
  app.use(
    helmet({
      // Allow preview audio (served from 4100) to be consumed by the Vite dev server (5173).
      crossOriginResourcePolicy: { policy: 'cross-origin' },
    })
  );
  app.use(
    helmet.contentSecurityPolicy({
      useDefaults: true,
      directives: {
        // Allow blob: for local audio previews (object URLs) while keeping other sources self-only.
        defaultSrc: ["'self'", 'blob:'],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        imgSrc: ["'self'", 'data:'],
        fontSrc: ["'self'", 'data:'],
        // Allow object-URL audio previews and preview WAVs fetched from allowed origins.
        mediaSrc: ["'self'", 'blob:', 'data:', ...allowedOrigins],
        connectSrc: ["'self'", ...allowedOrigins, ...allowedOrigins.map(toWsOrigin)],
        objectSrc: ["'none'"],
        baseUri: ["'self'"],
        frameAncestors: ["'self'"],
      },
    })
  );
  app.use(morgan('dev'));
  const staticDir = existsSync(path.resolve('client/dist')) ? 'client/dist' : 'public';
  app.use(express.static(path.resolve(staticDir)));

  app.get('/healthz', (_req, res) => {
    res.json({ status: 'ok', time: new Date().toISOString() });
  });

  app.get('/api/config', (_req, res) => {
    res.json({ audio: { chunkMs: config.audio.chunkMs } });
  });

  app.get('/api/providers', async (_req, res, next) => {
    try {
      const providers = await providerStatusCache.get();
      res.json(providers);
    } catch (error) {
      next(error as Error);
    }
  });

  app.post('/api/admin/reload-config', async (_req, res, next) => {
    try {
      reloadEnvironment();
      reloadConfig();
      config = await loadConfig();
      const refreshedRefreshMs = config.providerHealth?.refreshMs ?? 5_000;
      providerStatusCache.updateConfig(config, refreshedRefreshMs);
      const refreshedProviders = await providerStatusCache.refresh();
      res.json({
        status: 'ok',
        providers: refreshedProviders,
        note: 'storage/job-state stores are not re-instantiated; restart required to change storage settings',
      });
    } catch (error) {
      next(error as Error);
    }
  });

  app.post('/api/jobs/transcribe', upload.array('files'), async (req, res, next) => {
    let handedOffToRunner = false;
    try {
      const providerAvailability = await providerStatusCache.get();
      const parsed = parseBatchRequest(req, providerAvailability, config);
      for (const provider of parsed.providers) {
        const adapter = getAdapter(provider);
        if (!adapter.supportsBatch) {
          res
            .status(400)
            .json({ message: `Provider ${provider} does not support batch transcription` });
          return;
        }
      }
      const job = await batchRunner.enqueue(
        parsed.providers,
        parsed.lang,
        parsed.files.map((file) => ({ path: file.path, size: file.size, originalname: file.originalname })),
        parsed.manifest,
        parsed.options
      );
      handedOffToRunner = true;
      res.json(job);
    } catch (error) {
      if (!handedOffToRunner) {
        await cleanupUploadedFiles(req.files as Express.Multer.File[] | undefined);
      }
      if (error instanceof HttpError) {
        res.status(error.statusCode).json(error.payload ?? { message: error.message });
        return;
      }
      next(error);
    }
  });

  app.get('/api/jobs/:jobId/status', async (req, res, next) => {
    try {
      const status = await batchRunner.getStatusIncludingPersisted(req.params.jobId);
      if (!status) {
        res.status(404).json({ message: 'Job not found' });
        return;
      }
      res.json(status);
    } catch (error) {
      next(error as Error);
    }
  });

  app.get('/api/jobs/:jobId/results', async (req, res) => {
    const results = await batchRunner.getResults(req.params.jobId);
    if (!results) {
      res.status(404).json({ message: 'Job not found' });
      return;
    }
    const format = (req.query.format as string) ?? 'json';
    if (format === 'csv') {
      res.type('text/csv');
      const csvStream = toCsvStream(results);
      csvStream.on('error', (error) => {
        logger.error({
          event: 'csv_stream_failed',
          jobId: req.params.jobId,
          message: error instanceof Error ? error.message : 'csv stream failed',
        });
        if (!res.headersSent) {
          res.status(500).json({ message: 'failed to stream csv' });
          return;
        }
        res.destroy(error instanceof Error ? error : undefined);
      });
      csvStream.pipe(res);
      return;
    }
    res.json(results);
  });

  app.get('/api/jobs', async (req, res, next) => {
    try {
      const raw = Number(req.query.limit ?? 200);
      const limit = Number.isFinite(raw) ? Math.min(1000, Math.max(1, raw)) : 200;
      const entries = await batchRunner.listJobs(limit);
      res.json(entries);
    } catch (error) {
      next(error as Error);
    }
  });

  app.get('/api/jobs/:jobId/summary', async (req, res) => {
    try {
      const results = await batchRunner.getResults(req.params.jobId);
      if (!results) {
        res.status(404).json({ message: 'Job not found' });
        return;
      }
      const summary = summarizeJob(results);
      const groupBy = (req.query.groupBy as string) ?? 'aggregate';
      if (groupBy === 'provider') {
        res.json({ summary, byProvider: summarizeJobByProvider(results) });
        return;
      }
      res.json(summary);
    } catch (error) {
      res.status(500).json({ message: (error as Error).message });
    }
  });

  app.get('/api/realtime/latency', createRealtimeLatencyHandler(realtimeLatencyStore));
  app.get('/api/realtime/log-sessions', createRealtimeLogSessionsHandler(realtimeTranscriptLogStore));
  app.get('/api/realtime/logs/:sessionId', async (req, res, next) => {
    try {
      const { sessionId } = req.params;
      if (!sessionId) {
        res.status(400).json({ message: 'sessionId is required' });
        return;
      }
      const entries = await realtimeTranscriptLogStore.readSession(sessionId);
      if (entries.length === 0) {
        res.status(404).json({ message: 'session log not found' });
        return;
      }
      res.json(entries);
    } catch (error) {
      next(error as Error);
    }
  });

  app.get('/api/voice/status', (_req, res) => {
    try {
      const catalog = getVoicePresetCatalog(config);
      const available = catalog.presets.some((p) => p.available);
      const defaultPreset =
        (catalog.defaultPresetId ? catalog.presets.find((p) => p.id === catalog.defaultPresetId) : null) ??
        catalog.presets[0] ??
        null;
      res.json({
        available,
        defaultPresetId: catalog.defaultPresetId,
        presets: catalog.presets,
        // Backward-compatible fields (derived from the default preset).
        missing: defaultPreset ? [...defaultPreset.missingEnv, ...defaultPreset.issues] : [],
        providers: defaultPreset?.providers,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'invalid voice configuration';
      res.json({
        available: false,
        missing: [message],
        presets: [],
      });
    }
  });

  app.post('/api/realtime/preview', upload.single('file'), async (req, res, next) => {
    let normalization: Awaited<ReturnType<typeof ensureNormalizedAudio>> | null = null;
    try {
      if (!req.file) {
        throw new HttpError(400, 'audio file is required for preview');
      }
      if (!req.file.size || req.file.size === 0) {
        throw new HttpError(400, 'audio file is empty');
      }
      const maxBytes = config.ws?.preview?.maxUploadBytes ?? 120 * 1024 * 1024;
      if (req.file.size > maxBytes) {
        throw new HttpError(413, 'audio file too large for preview');
      }

      normalization = await ensureNormalizedAudio(req.file.path, {
        config,
        allowCache: false,
        allowFallback: true,
      });
      const item = previewStore.create(normalization.normalizedPath);
      res.json({
        previewId: item.id,
        previewUrl: `/api/realtime/preview/${item.id}`,
        degraded: normalization.degraded,
      });
    } catch (error) {
      if (error instanceof AudioValidationError || error instanceof AudioDecodeError) {
        const code = error instanceof AudioValidationError ? error.code : 'AUDIO_UNSUPPORTED_FORMAT';
        res.status(422).json({
          code,
          message: error.message,
          detail: error instanceof AudioValidationError ? error.detail ?? error.message : error.message,
        });
        return;
      }
      if (error instanceof HttpError) {
        res.status(error.statusCode).json(error.payload ?? { message: error.message });
        return;
      }
      next(error as Error);
    } finally {
      if (req.file?.path) {
        const shouldDeleteOriginal = !normalization || normalization.normalizedPath !== req.file.path;
        if (shouldDeleteOriginal) {
          await unlink(req.file.path).catch(() => undefined);
        }
      }
    }
  });

  app.get('/api/realtime/preview/:id', async (req, res, next) => {
    try {
      const { id } = req.params;
      if (!id) {
        res.status(400).json({ message: 'previewId is required' });
        return;
      }
      const item = previewStore.take(id);
      if (!item) {
        res.status(404).json({ message: 'preview not found or expired' });
        return;
      }
      res.setHeader('Content-Type', 'audio/wav');
      res.setHeader('Cache-Control', 'no-store');
      const stream = createReadStream(item.filePath);
      stream.on('close', () => {
        void unlink(item.filePath).catch(() => undefined);
      });
      stream.on('error', async (err) => {
        await unlink(item.filePath).catch(() => undefined);
        next(err);
      });
      stream.pipe(res);
    } catch (error) {
      next(error as Error);
    }
  });

  app.post('/api/realtime/replay', upload.single('file'), async (req, res, next) => {
    try {
      const providerAvailability = await providerStatusCache.get();
      const provider = req.body.provider as ProviderId | undefined;
      const providersRaw = req.body.providers as string | string[] | undefined;
      const lang = (req.body.lang as string) ?? '';
      if ((!provider && !providersRaw) || !lang) {
        throw new HttpError(400, 'provider/providers and lang are required for replay');
      }
      if (!req.file) {
        throw new HttpError(400, 'audio file is required for replay');
      }
      if (!req.file.size || req.file.size === 0) {
        throw new HttpError(400, 'audio file is empty');
      }
      const providers = (() => {
        if (providersRaw) {
          try {
            const parsed = Array.isArray(providersRaw)
              ? providersRaw
              : (() => {
                  const raw = providersRaw as string;
                  if (raw.trim().startsWith('[')) return JSON.parse(raw) as unknown;
                  return raw.split(',').map((v) => v.trim()).filter(Boolean);
                })();
            if (!Array.isArray(parsed) || parsed.length === 0) {
              throw new Error('providers must be a non-empty array');
            }
            return Array.from(new Set(parsed)) as ProviderId[];
          } catch (err) {
            throw new HttpError(400, `invalid providers: ${(err as Error).message}`);
          }
        }
        if (provider) return [provider];
        return [];
      })();

      if (providers.length === 0) {
        throw new HttpError(400, 'providers must not be empty');
      }

      const providerLimit = providers
        .map((p) => getBatchMaxBytes(p, config))
        .filter((v): v is number => typeof v === 'number')
        .sort((a, b) => a - b)[0];
      if (providerLimit && req.file.size > providerLimit) {
        throw new HttpError(
          413,
          `audio file too large for provider (${Math.round(providerLimit / (1024 * 1024))}MB limit)`
        );
      }
      // Guard extremely long inputs that can hang realtime sockets
      const maxBytes = config.ws?.replay?.maxUploadBytes ?? 1024 * 1024 * 200;
      if (req.file.size > maxBytes) {
        throw new HttpError(413, 'audio file too large for realtime replay');
      }
      providers.forEach((p) => requireProviderAvailable(providerAvailability, p, 'streaming'));
      const normalization = await ensureNormalizedAudio(req.file.path, { config, allowCache: false, allowFallback: true });
      const normalizedFile = {
        ...req.file,
        path: normalization.normalizedPath,
        size: normalization.bytes,
      };
      const session = replaySessionStore.create(normalizedFile, providers, lang);
      res.json({
        sessionId: session.id,
        filename: session.originalName,
        createdAt: session.createdAt,
        degraded: normalization.degraded,
      });
    } catch (error) {
      if (error instanceof AudioValidationError || error instanceof AudioDecodeError) {
        const code = error instanceof AudioValidationError ? error.code : 'AUDIO_UNSUPPORTED_FORMAT';
        res.status(422).json({
          code,
          message: error.message,
          detail: error instanceof AudioValidationError ? error.detail ?? error.message : error.message,
        });
        return;
      }
      if (error instanceof HttpError) {
        res.status(error.statusCode).json(error.payload ?? { message: error.message });
        return;
      }
      next(error);
    }
  });

  app.use((err: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    logger.error({ event: 'server_error', message: err.message });
    res.status(500).json({ message: err.message });
  });

  streamWss.on('connection', (ws, req) => {
    const url = new URL(req.url ?? '', 'http://localhost');
    const provider = url.searchParams.get('provider') as ProviderId;
    const lang = url.searchParams.get('lang') ?? 'ja-JP';
    const origin = req.headers.origin;
    const allowedOrigins = parseAllowedOrigins();
    const sendWsError = (message: string) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message }));
      }
      ws.close();
    };

    if (!isOriginAllowed(origin, allowedOrigins)) {
      sendWsError('origin not allowed');
      return;
    }
    if (!provider) {
      sendWsError('provider query param is required');
      return;
    }

    (async () => {
      const providerAvailability = await providerStatusCache.get();
      try {
        requireProviderAvailable(providerAvailability, provider, 'streaming');
      } catch (err) {
        sendWsError((err as Error).message);
        return;
      }

      try {
        await handleStreamConnection(
          ws,
          provider,
          lang,
          realtimeLatencyStore,
          realtimeTranscriptLogStore
        );
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error({ event: 'ws_handler_error', message: err.message });
        sendWsError(err.message ?? 'streaming handler error');
      }
    })().catch((error) => {
      const err = error instanceof Error ? error : new Error(String(error));
      logger.error({ event: 'ws_handler_error', message: err.message });
      sendWsError(err.message ?? 'streaming connection error');
    });
  });

  const streamCompareWss = new WebSocketServer({ noServer: true });

  streamCompareWss.on('connection', (ws, req) => {
    const url = new URL(req.url ?? '', 'http://localhost');
    const providersParam = url.searchParams.get('providers');
    const lang = url.searchParams.get('lang') ?? 'ja-JP';
    const origin = req.headers.origin;
    const allowedOrigins = parseAllowedOrigins();

    const sendWsError = (message: string) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message }));
      }
      ws.close();
    };

    if (!isOriginAllowed(origin, allowedOrigins)) {
      sendWsError('origin not allowed');
      return;
    }

    const providers = (providersParam ?? '')
      .split(',')
      .map((p) => p.trim())
      .filter((p) => p.length > 0) as ProviderId[];

    if (providers.length < 2) {
      sendWsError('providers query param must include at least two entries');
      return;
    }

    (async () => {
      const providerAvailability = await providerStatusCache.get();
      try {
        providers.forEach((provider) => {
          requireProviderAvailable(providerAvailability, provider, 'streaming');
        });
      } catch (err) {
        sendWsError((err as Error).message);
        return;
      }

      try {
        await handleCompareStreamConnection(
          ws,
          providers,
          lang,
          realtimeLatencyStore,
          realtimeTranscriptLogStore
        );
      } catch (error) {
        logger.error({ event: 'ws_handler_error', message: (error as Error).message });
        sendWsError((error as Error).message ?? 'streaming compare handler error');
      }
    })().catch((error) => {
      logger.error({ event: 'ws_handler_error', message: error.message });
      sendWsError(error.message ?? 'streaming compare connection error');
    });
  });

  const replayWss = new WebSocketServer({ noServer: true });

  replayWss.on('connection', (ws, req) => {
    const url = new URL(req.url ?? '', 'http://localhost');
    const provider = url.searchParams.get('provider') as ProviderId | null;
    const providersParam = url.searchParams.get('providers');
    const lang = url.searchParams.get('lang') ?? 'ja-JP';
    const sessionId = url.searchParams.get('sessionId');
    const origin = req.headers.origin;
    const allowedOrigins = parseAllowedOrigins();

    const sendWsError = (message: string) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message }));
      }
      ws.close();
    };

    if (!isOriginAllowed(origin, allowedOrigins)) {
      sendWsError('origin not allowed');
      return;
    }
    if (!sessionId) {
      sendWsError('sessionId query param is required');
      return;
    }

    const providers = (() => {
      if (providersParam) {
        return providersParam
          .split(',')
          .map((p) => p.trim())
          .filter(Boolean) as ProviderId[];
      }
      return provider ? [provider] : [];
    })();

    if (providers.length === 0) {
      sendWsError('provider or providers query param is required');
      return;
    }

    const isMulti = providers.length >= 2;

    (async () => {
      const providerAvailability = await providerStatusCache.get();
      try {
        providers.forEach((p) => requireProviderAvailable(providerAvailability, p, 'streaming'));
        if (isMulti) {
          await handleReplayMultiConnection(
            ws,
            providers,
            lang,
            sessionId,
            replaySessionStore,
            realtimeLatencyStore,
            realtimeTranscriptLogStore
          );
        } else {
          await handleReplayConnection(
            ws,
            providers[0],
            lang,
            sessionId,
            replaySessionStore,
            realtimeLatencyStore,
            realtimeTranscriptLogStore
          );
        }
      } catch (error) {
        logger.error({ event: 'ws_replay_error', message: (error as Error).message });
        sendWsError((error as Error).message ?? 'replay handler error');
      }
    })().catch((error) => {
        logger.error({ event: 'ws_replay_error', message: error.message });
        sendWsError(error.message ?? 'replay connection error');
      });
  });

  const voiceWss = new WebSocketServer({ noServer: true });

  voiceWss.on('connection', (ws, req) => {
    const url = new URL(req.url ?? '', 'http://localhost');
    const lang = url.searchParams.get('lang') ?? 'ja-JP';
    const origin = req.headers.origin;
    const allowedOrigins = parseAllowedOrigins();
    const sendWsError = (message: string) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message }));
      }
      ws.close();
    };

    if (!isOriginAllowed(origin, allowedOrigins)) {
      sendWsError('origin not allowed');
      return;
    }

    try {
      const catalog = getVoicePresetCatalog(config);
      const hasAnyPreset = catalog.presets.length > 0;
      const hasAvailable = catalog.presets.some((p) => p.available);
      if (!hasAnyPreset) {
        sendWsError('voice agent unavailable: no presets configured');
        return;
      }
      if (!hasAvailable) {
        sendWsError('voice agent unavailable: no presets available (check /api/voice/status)');
        return;
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'invalid voice configuration';
      sendWsError(`voice agent unavailable: ${message}`);
      return;
    }

    void handleVoiceConnection(ws, lang).catch((error) => {
      logger.error({ event: 'ws_voice_error', message: error.message });
      sendWsError(error.message ?? 'voice handler error');
    });
  });

  server.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url ?? '', 'http://localhost');
    if (url.pathname === '/ws/stream') {
      streamWss.handleUpgrade(req, socket, head, (ws) => streamWss.emit('connection', ws, req));
      return;
    }
    if (url.pathname === '/ws/stream/compare') {
      streamCompareWss.handleUpgrade(req, socket, head, (ws) => streamCompareWss.emit('connection', ws, req));
      return;
    }
    if (url.pathname === '/ws/replay') {
      replayWss.handleUpgrade(req, socket, head, (ws) => replayWss.emit('connection', ws, req));
      return;
    }
    if (url.pathname === '/ws/voice') {
      voiceWss.handleUpgrade(req, socket, head, (ws) => voiceWss.emit('connection', ws, req));
      return;
    }
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
  });

  const port = Number(
    process.env.TEST_SERVER_PORT ?? process.env.SERVER_PORT ?? process.env.PORT ?? 4100
  );
  server.listen(port, () => {
    logger.info({ event: 'server_started', port });
    console.log(`Server listening on http://localhost:${port}`);
  });
}

if (process.env.NODE_ENV !== 'test') {
  bootstrap().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
