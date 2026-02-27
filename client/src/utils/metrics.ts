import type { FileResult, JobSummary, SummaryStats } from '../types/app';

export const fmt = (value: number | null | undefined): string => {
  if (value == null) return '-';
  return value.toFixed(3);
};

export const summarizeMetric = (values: Array<number | null | undefined>): SummaryStats => {
  const nums = values.filter((v): v is number => typeof v === 'number' && Number.isFinite(v));
  if (nums.length === 0) return { n: 0, avg: null, p50: null, p95: null };
  const sorted = [...nums].sort((a, b) => a - b);
  const quantile = (q: number) => {
    const pos = (sorted.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    if (sorted[base + 1] !== undefined) return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
    return sorted[base];
  };
  const avg = nums.reduce((acc, curr) => acc + curr, 0) / nums.length;
  return { n: nums.length, avg, p50: quantile(0.5), p95: quantile(0.95) };
};

export const summarizeJobLocal = (rows: FileResult[]): JobSummary => {
  const degradedCount = rows.reduce((count, row) => (row.degraded ? count + 1 : count), 0);
  return {
    count: rows.length,
    cer: summarizeMetric(rows.map((r) => r.cer)),
    wer: summarizeMetric(rows.map((r) => r.wer)),
    rtf: summarizeMetric(rows.map((r) => r.rtf)),
    latencyMs: summarizeMetric(rows.map((r) => r.latencyMs)),
    degraded: {
      count: degradedCount,
      ratio: rows.length > 0 ? degradedCount / rows.length : 0,
    },
  };
};
