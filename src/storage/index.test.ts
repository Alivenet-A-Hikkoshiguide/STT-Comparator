import { access, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { describe, expect, it } from 'vitest';
import {
  createRealtimeStorage,
  createRealtimeTranscriptStore,
  createStorage,
} from './index.js';

let sqliteAvailable = true;
try {
  const probe = new Database(':memory:');
  probe.close();
} catch {
  sqliteAvailable = false;
}
const maybeIt = sqliteAvailable ? it : it.skip;

describe('storage factories', () => {
  maybeIt('uses dedicated sqlite files per workload', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'storage-factory-'));
    try {
      const batchStore = createStorage('sqlite', dir);
      const realtimeStore = createRealtimeStorage('sqlite', dir);
      const transcriptStore = createRealtimeTranscriptStore('sqlite', dir);

      await batchStore.init();
      await realtimeStore.init();
      await transcriptStore.init();

      await expect(access(path.join(dir, 'results.sqlite'))).resolves.toBeUndefined();
      await expect(access(path.join(dir, 'realtime.sqlite'))).resolves.toBeUndefined();
      await expect(access(path.join(dir, 'realtime-logs.sqlite'))).resolves.toBeUndefined();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
