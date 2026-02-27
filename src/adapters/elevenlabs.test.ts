import { Readable } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const wsInstances: Array<{ url: string }> = [];

vi.mock('ws', () => {
  const { EventEmitter } = require('node:events');
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;
    readyState = FakeWebSocket.OPEN;
    url: string;
    send = vi.fn();
    close = vi.fn(() => this.emit('close'));

    constructor(url: string) {
      super();
      this.url = url;
      wsInstances.push(this);
      queueMicrotask(() => {
        this.emit('open');
      });
    }
  }
  return { WebSocket: FakeWebSocket };
});

function toPcmStream(buffer: Buffer): Readable {
  return new Readable({
    read() {
      this.push(buffer);
      this.push(null);
    },
  });
}

describe('ElevenLabsAdapter model selection', () => {
  const globalWithFetch = globalThis as typeof globalThis & { fetch: typeof fetch };
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalWithFetch.fetch;
    process.env.ELEVENLABS_API_KEY = 'dummy';
    delete process.env.ELEVENLABS_STT_STREAMING_MODEL_ID;
    delete process.env.ELEVENLABS_STT_BATCH_MODEL_ID;
    wsInstances.length = 0;
  });

  afterEach(() => {
    globalWithFetch.fetch = originalFetch;
    vi.clearAllMocks();
    vi.resetModules();
  });

  it('uses scribe_v2_realtime for streaming by default', async () => {
    const { ElevenLabsAdapter } = await import('./elevenlabs.js');
    const adapter = new ElevenLabsAdapter();
    await adapter.startStreaming({
      language: 'en',
      sampleRateHz: 16000,
      encoding: 'linear16',
    });

    const ws = wsInstances[0];
    expect(ws).toBeDefined();
    const url = new URL(ws.url);
    expect(url.searchParams.get('model_id')).toBe('scribe_v2_realtime');
  });

  it('allows overriding streaming model via env', async () => {
    process.env.ELEVENLABS_STT_STREAMING_MODEL_ID = 'scribe_v2_experimental';

    const { ElevenLabsAdapter } = await import('./elevenlabs.js');
    const adapter = new ElevenLabsAdapter();
    await adapter.startStreaming({
      language: 'en',
      sampleRateHz: 16000,
      encoding: 'linear16',
    });

    const ws = wsInstances[0];
    expect(ws).toBeDefined();
    const url = new URL(ws.url);
    expect(url.searchParams.get('model_id')).toBe('scribe_v2_experimental');
  });

  it('uses scribe_v2 for batch by default', async () => {
    const fakeResponse = {
      ok: true,
      status: 200,
      json: vi.fn(async () => ({ text: 'ok' })),
      text: vi.fn(async () => ''),
    } as unknown as Response;
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse);
    globalWithFetch.fetch = fetchMock as unknown as typeof fetch;

    const { ElevenLabsAdapter } = await import('./elevenlabs.js');
    const adapter = new ElevenLabsAdapter();
    await adapter.transcribeFileFromPCM(toPcmStream(Buffer.from([1, 2, 3, 4])), {
      language: 'en',
      sampleRateHz: 16000,
      encoding: 'linear16',
    });

    const [, requestInit] = fetchMock.mock.calls[0] ?? [];
    expect(requestInit).toBeDefined();
    const bodyBytes = (requestInit as RequestInit).body;
    expect(bodyBytes).toBeInstanceOf(Uint8Array);
    const bodyText = Buffer.from(bodyBytes as Uint8Array).toString('latin1');
    expect(bodyText).toContain('name="model_id"\r\n\r\nscribe_v2\r\n');
  });

  it('allows overriding batch model via env', async () => {
    process.env.ELEVENLABS_STT_BATCH_MODEL_ID = 'scribe_v2_experimental';
    const fakeResponse = {
      ok: true,
      status: 200,
      json: vi.fn(async () => ({ text: 'ok' })),
      text: vi.fn(async () => ''),
    } as unknown as Response;
    const fetchMock = vi.fn().mockResolvedValue(fakeResponse);
    globalWithFetch.fetch = fetchMock as unknown as typeof fetch;

    const { ElevenLabsAdapter } = await import('./elevenlabs.js');
    const adapter = new ElevenLabsAdapter();
    await adapter.transcribeFileFromPCM(toPcmStream(Buffer.from([1, 2, 3, 4])), {
      language: 'en',
      sampleRateHz: 16000,
      encoding: 'linear16',
    });

    const [, requestInit] = fetchMock.mock.calls[0] ?? [];
    expect(requestInit).toBeDefined();
    const bodyBytes = (requestInit as RequestInit).body;
    expect(bodyBytes).toBeInstanceOf(Uint8Array);
    const bodyText = Buffer.from(bodyBytes as Uint8Array).toString('latin1');
    expect(bodyText).toContain('name="model_id"\r\n\r\nscribe_v2_experimental\r\n');
  });
});
