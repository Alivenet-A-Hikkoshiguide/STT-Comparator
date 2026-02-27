import path from 'node:path';
import { JsonlStore } from './jsonlStore.js';
import { SqliteStore } from './sqliteStore.js';
import { RealtimeSqliteStore } from './realtimeSqliteStore.js';
import { RealtimeTranscriptStore, type RealtimeTranscriptLogStore } from './realtimeTranscriptStore.js';
import { RealtimeTranscriptSqliteStore } from './realtimeTranscriptSqliteStore.js';
import type { StorageDriver, StorageDriverName, BatchJobFileResult, RealtimeLatencySummary } from '../types.js';
import type { RetentionPolicy } from './retention.js';

export function createStorage(
  driver: StorageDriverName,
  storagePath: string,
  retention?: RetentionPolicy
): StorageDriver<BatchJobFileResult> {
  if (driver === 'jsonl') {
    return new JsonlStore<BatchJobFileResult>(path.resolve(storagePath, 'results.jsonl'), retention);
  }
  const dbPath = path.resolve(storagePath, 'results.sqlite');
  return new SqliteStore(dbPath, undefined, retention);
}

export function createRealtimeStorage(
  driver: StorageDriverName,
  storagePath: string,
  retention?: RetentionPolicy
): StorageDriver<RealtimeLatencySummary> {
  if (driver === 'jsonl') {
    return new JsonlStore<RealtimeLatencySummary>(
      path.resolve(storagePath, 'realtime.jsonl'),
      retention
    );
  }
  const dbPath = path.resolve(storagePath, 'realtime.sqlite');
  return new RealtimeSqliteStore(dbPath, undefined, retention);
}

export function createRealtimeTranscriptStore(
  driver: StorageDriverName,
  storagePath: string,
  retention?: RetentionPolicy
): RealtimeTranscriptLogStore {
  if (driver === 'jsonl') {
    const filePath = path.resolve(storagePath, 'realtime-logs.jsonl');
    return new RealtimeTranscriptStore(filePath, retention);
  }
  const dbPath = path.resolve(storagePath, 'realtime-logs.sqlite');
  return new RealtimeTranscriptSqliteStore(dbPath, undefined, retention);
}
