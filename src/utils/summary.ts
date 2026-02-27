import type { BatchJobFileResult } from '../types.js';

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

export interface JobSummary {
  count: number;
  cer: SummaryStats;
  wer: SummaryStats;
  rtf: SummaryStats;
  latencyMs: SummaryStats;
  degraded: DegradedSummary;
}

function quantileSorted(sorted: number[], q: number): number | null {
  if (sorted.length === 0) return null;
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  const left = sorted[base];
  const right = sorted[base + 1];
  if (right !== undefined) {
    return left + rest * (right - left);
  }
  return left;
}

function summarize(values: Array<number | undefined | null>): SummaryStats {
  const nums: number[] = [];
  let sum = 0;
  for (const value of values) {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      continue;
    }
    nums.push(value);
    sum += value;
  }
  if (nums.length === 0) return { n: 0, avg: null, p50: null, p95: null };
  nums.sort((a, b) => a - b);
  const avg = sum / nums.length;
  return {
    n: nums.length,
    avg,
    p50: quantileSorted(nums, 0.5),
    p95: quantileSorted(nums, 0.95),
  };
}

export function summarizeJob(results: BatchJobFileResult[]): JobSummary {
  const degradedCount = results.reduce((count, row) => (row.degraded ? count + 1 : count), 0);
  return {
    count: results.length,
    cer: summarize(results.map((r) => r.cer)),
    wer: summarize(results.map((r) => r.wer)),
    rtf: summarize(results.map((r) => r.rtf)),
    latencyMs: summarize(results.map((r) => r.latencyMs)),
    degraded: {
      count: degradedCount,
      ratio: results.length > 0 ? degradedCount / results.length : 0,
    },
  };
}

export function summarizeJobByProvider(
  results: BatchJobFileResult[]
): Record<string, JobSummary> {
  const grouped = results.reduce<Record<string, BatchJobFileResult[]>>((acc, row) => {
    const key = row.provider;
    (acc[key] ??= []).push(row);
    return acc;
  }, {});
  return Object.fromEntries(
    Object.entries(grouped).map(([provider, rows]) => [provider, summarizeJob(rows)])
  );
}
