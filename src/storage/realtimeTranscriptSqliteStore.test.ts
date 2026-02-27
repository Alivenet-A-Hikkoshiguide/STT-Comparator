import path from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import Database from 'better-sqlite3';
import { describe, it, expect } from 'vitest';
import { RealtimeTranscriptSqliteStore } from './realtimeTranscriptSqliteStore.js';
import type { RealtimeTranscriptLogEntry } from '../types.js';

let sqliteAvailable = true;
try {
  const probe = new Database(':memory:');
  probe.close();
} catch {
  sqliteAvailable = false;
}
const maybeIt = sqliteAvailable ? it : it.skip;

describe('RealtimeTranscriptSqliteStore', () => {
  const makeDbPath = () => path.join(tmpdir(), `realtime-logs-${randomUUID()}.sqlite`);

  maybeIt('persists and reads entries with speakerId', async () => {
    const dbPath = makeDbPath();
    const store = new RealtimeTranscriptSqliteStore(dbPath);
    await store.init();

    const entry: RealtimeTranscriptLogEntry = {
      sessionId: 's1',
      provider: 'mock',
      lang: 'ja-JP',
      recordedAt: new Date().toISOString(),
      payload: { type: 'transcript', provider: 'mock', isFinal: true, text: 'hello', timestamp: Date.now(), channel: 'mic', speakerId: 'L' },
    };

    await store.append(entry);
    const rows = await store.readSession('s1');
    expect(rows).toHaveLength(1);
    expect(rows[0].payload).toEqual(entry.payload);
  });

  maybeIt('adds speakerId column when missing', async () => {
    const dbPath = makeDbPath();
    const db = new Database(dbPath);
    db.prepare(
      `CREATE TABLE realtime_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sessionId TEXT,
        provider TEXT,
        lang TEXT,
        recordedAt TEXT,
        payload TEXT
      )`
    ).run();
    db.close();

    const store = new RealtimeTranscriptSqliteStore(dbPath);
    await store.init();

    const columns = new Database(dbPath)
      .prepare(`PRAGMA table_info('realtime_logs')`)
      .all() as Array<{ name: string }>;
    expect(columns.some((col) => col.name === 'speakerId')).toBe(true);
  });
});
