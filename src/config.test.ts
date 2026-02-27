import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { loadConfig, reloadConfig } from './config.js';

const formatLocalDate = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

describe('loadConfig', () => {
  let tempDir: string | null = null;

  beforeEach(() => {
    reloadConfig();
  });

  afterEach(async () => {
    reloadConfig();
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true });
      tempDir = null;
    }
  });

  it('expands {date} in storage.path', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'stt-config-'));
    const configPath = path.join(tempDir, 'config.json');
    const payload = {
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      normalization: { nfkc: true, stripPunct: true, stripSpace: false, lowercase: false },
      storage: { driver: 'jsonl', path: './runs/{date}', retentionDays: 30, maxRows: 100000 },
      providers: ['mock'],
    };
    await writeFile(configPath, JSON.stringify(payload), 'utf-8');

    const config = await loadConfig(configPath);
    expect(config.storage.path).toBe(`./runs/${formatLocalDate()}`);
  });

  it('applies default jobs config when jobs is omitted', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'stt-config-'));
    const configPath = path.join(tempDir, 'config.json');
    const payload = {
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      normalization: { nfkc: true, stripPunct: true, stripSpace: false, lowercase: false },
      storage: { driver: 'jsonl', path: './runs/{date}', retentionDays: 30, maxRows: 100000 },
      providers: ['mock'],
    };
    await writeFile(configPath, JSON.stringify(payload), 'utf-8');

    const config = await loadConfig(configPath);
    expect(config.jobs.maxParallel).toBe(4);
    expect(config.jobs.retentionMs).toBe(10 * 60 * 1000);
    expect(config.jobs.persistenceMode).toBe('required');
    expect(config.jobs.stateDbPath).toBe('./runs/job-state/batch-jobs.sqlite');
    expect(config.jobs.retry.maxAttempts).toBe(3);
    expect(config.jobs.retry.baseDelayMs).toBe(1000);
    expect(config.jobs.retry.maxDelayMs).toBe(10 * 1000);
  });

  it('rejects jobs.stateDbPath containing {date}', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'stt-config-'));
    const configPath = path.join(tempDir, 'config.json');
    const payload = {
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      normalization: { nfkc: true, stripPunct: true, stripSpace: false, lowercase: false },
      storage: { driver: 'jsonl', path: './runs/{date}', retentionDays: 30, maxRows: 100000 },
      providers: ['mock'],
      jobs: {
        stateDbPath: './runs/{date}/batch-jobs.sqlite',
      },
    };
    await writeFile(configPath, JSON.stringify(payload), 'utf-8');

    await expect(loadConfig(configPath)).rejects.toThrow('jobs.stateDbPath must not include {date}');
  });

  it('rejects jobs.persistenceMode=best_effort', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'stt-config-'));
    const configPath = path.join(tempDir, 'config.json');
    const payload = {
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      normalization: { nfkc: true, stripPunct: true, stripSpace: false, lowercase: false },
      storage: { driver: 'jsonl', path: './runs/{date}', retentionDays: 30, maxRows: 100000 },
      providers: ['mock'],
      jobs: {
        persistenceMode: 'best_effort',
      },
    };
    await writeFile(configPath, JSON.stringify(payload), 'utf-8');

    await expect(loadConfig(configPath)).rejects.toThrow('invalid_literal');
  });

  it('rejects non-16k sample rate for fairness', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'stt-config-'));
    const configPath = path.join(tempDir, 'config.json');
    const payload = {
      audio: { targetSampleRate: 24000, targetChannels: 1, chunkMs: 250 },
      normalization: { nfkc: true, stripPunct: true, stripSpace: false, lowercase: false },
      storage: { driver: 'jsonl', path: './runs/{date}', retentionDays: 30, maxRows: 100000 },
      providers: ['mock'],
    };
    await writeFile(configPath, JSON.stringify(payload), 'utf-8');

    await expect(loadConfig(configPath)).rejects.toThrow('expected 16000');
  });

  it('rejects ingressNormalize channel overrides that break mono baseline', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'stt-config-'));
    const configPath = path.join(tempDir, 'config.json');
    const payload = {
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      ingressNormalize: { enabled: true, targetSampleRate: 16000, targetChannels: 2 },
      normalization: { nfkc: true, stripPunct: true, stripSpace: false, lowercase: false },
      storage: { driver: 'jsonl', path: './runs/{date}', retentionDays: 30, maxRows: 100000 },
      providers: ['mock'],
    };
    await writeFile(configPath, JSON.stringify(payload), 'utf-8');

    await expect(loadConfig(configPath)).rejects.toThrow('ingressNormalize.targetChannels must be 1');
  });
});
