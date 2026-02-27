import { Readable } from 'node:stream';
import type { BatchJobFileResult } from '../types.js';

function csvEscape(value: unknown): string {
  const str = String(value ?? '');
  if (str.includes(',') || str.includes('\n') || str.includes('"')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

const CSV_HEADER = [
  'path',
  'provider',
  'lang',
  'cer',
  'wer',
  'rtf',
  'latency_ms',
  'audio_sample_rate_hz',
  'audio_channels',
  'audio_encoding',
  'degraded',
  'normalization',
  'text',
  'ref_text',
];

function toCsvRow(row: BatchJobFileResult): string {
  return [
    csvEscape(row.path),
    row.provider,
    row.lang,
    row.cer ?? '',
    row.wer ?? '',
    row.rtf ?? '',
    row.latencyMs ?? '',
    row.audioSpec?.sampleRateHz ?? '',
    row.audioSpec?.channels ?? '',
    row.audioSpec?.encoding ?? '',
    row.degraded ? 'true' : '',
    row.normalizationUsed ? csvEscape(JSON.stringify(row.normalizationUsed)) : '',
    csvEscape(row.text),
    csvEscape(row.refText),
  ].join(',');
}

export function toCsv(rows: BatchJobFileResult[]): string {
  const lines = rows.map((row) => toCsvRow(row));
  return [CSV_HEADER.join(','), ...lines].join('\n');
}

export function toCsvStream(rows: Iterable<BatchJobFileResult>): Readable {
  return Readable.from(
    (function* streamLines() {
      yield `${CSV_HEADER.join(',')}\n`;
      for (const row of rows) {
        yield `${toCsvRow(row)}\n`;
      }
    })()
  );
}
