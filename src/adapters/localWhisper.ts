import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { buffer as streamToBuffer } from 'node:stream/consumers';
import { pipeline } from 'node:stream/promises';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import type { BatchResult, StreamingOptions, StreamingSession, TranscriptWord } from '../types.js';
import { BaseAdapter } from './base.js';
import { normalizeWhisperLanguage } from '../utils/language.js';
import { getWhisperRuntime } from '../utils/whisper.js';

const PCM_TO_WAV_TIMEOUT_MS = 2 * 60 * 1000;
const WHISPER_TIMEOUT_MS = 10 * 60 * 1000;

const PY_SCRIPT = `
import json, os, sys, time
try:
    import whisper
except Exception as e:
    sys.stderr.write(f"import whisper failed: {e}\\n")
    sys.exit(2)

audio_path = sys.argv[1]
lang = sys.argv[2] or None
model_name = os.getenv("WHISPER_MODEL", "small")
device = os.getenv("WHISPER_DEVICE", "cpu")

load_t0 = time.time()
model = whisper.load_model(model_name, device=device)
load_ms = int((time.time() - load_t0) * 1000)

t0 = time.time()
result = model.transcribe(audio_path, language=lang, fp16=(device != "cpu"), word_timestamps=True)
proc_ms = int((time.time() - t0) * 1000)

segments = result.get("segments") or []
words = []
for seg in segments:
    for w in seg.get("words") or []:
        words.append({
            "startSec": float(w.get("start", 0.0)),
            "endSec": float(w.get("end", 0.0)),
            "text": (w.get("word") or "").strip(),
            "confidence": float(w.get("probability")) if w.get("probability") is not None else None,
        })

duration = 0.0
if segments:
    duration = float(segments[-1].get("end") or segments[-1].get("start") or 0.0)
duration = max(duration, float(result.get("duration") or 0.0))

payload = {
    "text": (result.get("text") or "").strip(),
    "durationSec": duration,
    "vendorProcessingMs": proc_ms,
    "modelLoadMs": load_ms,
    "language": result.get("language"),
    "words": words,
}
print(json.dumps(payload, ensure_ascii=False))
`; // keep one-line output for JSON.parse

export class LocalWhisperAdapter extends BaseAdapter {
  id = 'local_whisper' as const;
  supportsStreaming = false;
  supportsBatch = true;

  async startStreaming(): Promise<StreamingSession> {
    throw new Error('Local Whisper adapter does not support streaming');
  }

  async transcribeFileFromPCM(pcm: NodeJS.ReadableStream, opts: StreamingOptions): Promise<BatchResult> {
    const runtime = getWhisperRuntime();
    if (!runtime.pythonPath) {
      throw new Error(runtime.reason ?? 'Whisper runtime is not available');
    }

    const pcmBuffer = await streamToBuffer(pcm);
    const whisperLanguage = normalizeWhisperLanguage(opts.language);
    let wavPath: string | null = null;
    try {
      wavPath = await this.toWavFile(Readable.from(pcmBuffer), opts.sampleRateHz, opts.encoding);
      const result = await this.runWhisper(runtime.pythonPath, wavPath, whisperLanguage ?? '');
      return {
        provider: this.id,
        text: result.text ?? '',
        words: result.words,
        durationSec: typeof result.durationSec === 'number' ? result.durationSec : undefined,
        vendorProcessingMs: typeof result.vendorProcessingMs === 'number' ? result.vendorProcessingMs : undefined,
      } satisfies BatchResult;
    } catch (error) {
      throw error instanceof Error ? error : new Error(typeof error === 'string' ? error : 'local_whisper failed');
    } finally {
      if (wavPath) {
        await unlink(wavPath).catch(() => undefined);
      }
    }
  }

  private async toWavFile(
    pcm: NodeJS.ReadableStream,
    sampleRateHz: number,
    encoding: StreamingOptions['encoding']
  ): Promise<string> {
    if (encoding !== 'linear16') {
      throw new Error(`Unsupported PCM encoding: ${encoding}`);
    }
    const wavPath = path.join(tmpdir(), `whisper-${randomUUID()}.wav`);
    const ffmpeg = spawn(
      ffmpegInstaller.path,
      [
        '-f',
        's16le',
        '-ar',
        String(sampleRateHz),
        '-ac',
        '1',
        '-i',
        'pipe:0',
        '-y',
        '-f',
        'wav',
        wavPath,
      ],
      { stdio: ['pipe', 'ignore', 'inherit'] }
    );

    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      ffmpeg.kill('SIGKILL');
    }, PCM_TO_WAV_TIMEOUT_MS);
    try {
      await pipeline(pcm, ffmpeg.stdin as NodeJS.WritableStream);
      const [code] = (await once(ffmpeg, 'close')) as [number | null];
      if (code !== 0) {
        if (timedOut) {
          throw new Error(`ffmpeg timeout after ${PCM_TO_WAV_TIMEOUT_MS}ms`);
        }
        throw new Error(`ffmpeg exited with code ${code ?? 'unknown'}`);
      }
      return wavPath;
    } finally {
      clearTimeout(timeout);
    }
  }

  private async runWhisper(pythonPath: string, wavPath: string, language: string): Promise<WhisperRunResult> {
    const whisperEnv = {
      ...process.env,
      PYTHONUNBUFFERED: '1',
      PATH: `${path.dirname(ffmpegInstaller.path)}:${process.env.PATH ?? ''}`,
      FFMPEG_BINARY: ffmpegInstaller.path,
    };
    const child = spawn(pythonPath, ['-u', '-c', PY_SCRIPT, wavPath, language ?? ''], {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: whisperEnv,
    });

    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr?.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, WHISPER_TIMEOUT_MS);
    const [code] = (await once(child, 'close')) as [number | null];
    clearTimeout(timer);

    if (code !== 0) {
      if (timedOut) {
        throw new Error(`Whisper process timeout after ${WHISPER_TIMEOUT_MS}ms`);
      }
      throw new Error(`Whisper process failed (code ${code ?? 'unknown'}): ${stderr || stdout}`);
    }

    const payloadText = stdout.trim();
    if (!payloadText) {
      throw new Error(`Whisper returned empty output${stderr ? `: ${stderr}` : ''}`);
    }

    return parseWhisperResult(payloadText);
  }
}

interface WhisperRunResult {
  text: string;
  words?: TranscriptWord[];
  durationSec?: number;
  vendorProcessingMs?: number;
  language?: string | null;
}

type WhisperWordSource = {
  start?: number;
  end?: number;
  startSec?: number;
  endSec?: number;
  t0?: number;
  t1?: number;
  word?: string;
  text?: string;
  confidence?: number | null;
};

const toTranscriptWord = (word: WhisperWordSource): TranscriptWord => ({
  startSec: Number(word.startSec ?? word.start ?? word.t0 ?? 0),
  endSec: Number(word.endSec ?? word.end ?? word.t1 ?? 0),
  text: String(word.text ?? word.word ?? '').trim(),
  confidence: typeof word.confidence === 'number' ? word.confidence : undefined,
});

function parseWhisperResult(raw: string): WhisperRunResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(`Failed to parse Whisper output: ${(err as Error).message}. Output: ${raw}`);
  }
  if (!parsed || typeof parsed !== 'object') {
    throw new Error(`Whisper returned unexpected payload: ${raw}`);
  }
  const payload = parsed as Record<string, unknown>;
  const words =
    Array.isArray(payload.words)
      ? payload.words
          .filter((w): w is WhisperWordSource => typeof w === 'object' && w !== null)
          .map(toTranscriptWord)
      : undefined;
  return {
    text: typeof payload.text === 'string' ? payload.text : '',
    words,
    durationSec: typeof payload.durationSec === 'number' ? payload.durationSec : undefined,
    vendorProcessingMs: typeof payload.vendorProcessingMs === 'number' ? payload.vendorProcessingMs : undefined,
    language: typeof payload.language === 'string' ? payload.language : null,
  };
}
