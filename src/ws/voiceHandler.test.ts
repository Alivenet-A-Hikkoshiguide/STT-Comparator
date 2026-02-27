import { describe, expect, it, vi, afterEach, beforeEach } from 'vitest';
import { EventEmitter } from 'node:events';
import type * as FsPromises from 'node:fs/promises';

describe('handleVoiceConnection', () => {
  class FakeWebSocket extends EventEmitter {
    sent: Array<string | Buffer> = [];
    closed = false;
    send(data: string | Buffer) {
      this.sent.push(data);
    }
    close() {
      if (this.closed) return;
      this.closed = true;
      this.emit('close');
    }
  }

  const state = vi.hoisted(() => {
    let onDataHandler: ((t: any) => void) | null = null;
    let onDataHandlers: Array<(t: any) => void> = [];
    let chatQueue: Array<() => Promise<{ text: string; citations: any[] }>> = [];
    const chatSignals: AbortSignal[] = [];
    const chatSnapshots: unknown[] = [];
    const controller = {
      sendAudio: vi.fn(async () => {}),
      end: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
    };
    const generateChatReply = vi.fn(async (_messages: unknown, opts?: { signal?: AbortSignal }) => {
      if (Array.isArray(_messages)) {
        chatSnapshots.push(_messages.map((m) => (m && typeof m === 'object' ? { ...(m as any) } : m)));
      } else {
        chatSnapshots.push(_messages);
      }
      if (opts?.signal) chatSignals.push(opts.signal);
      const next = chatQueue.shift();
      if (next) return await next();
      return { text: '了解しました。', citations: [] };
    });
    return {
      controller,
      setOnData: (cb: (t: any) => void) => {
        onDataHandler = cb;
        onDataHandlers.push(cb);
      },
      getOnData: () => onDataHandler,
      getOnDataAll: () => onDataHandlers.slice(),
      setChatQueue: (queue: Array<() => Promise<{ text: string; citations: any[] }>>) => {
        chatQueue = [...queue];
      },
      getChatSignals: () => chatSignals.slice(),
      getChatSnapshots: () => chatSnapshots.slice(),
      generateChatReply,
      reset: () => {
        onDataHandler = null;
        onDataHandlers = [];
        chatQueue = [];
        chatSignals.length = 0;
        chatSnapshots.length = 0;
      },
    };
  });

  const realtimeState = vi.hoisted(() => {
    let handlers: any = null;
    const session = {
      ready: Promise.resolve(),
      appendAudio: vi.fn(async () => {}),
      cancelResponse: vi.fn(async () => {}),
      truncateOutputAudio: vi.fn(async () => {}),
      close: vi.fn(async () => {}),
    };
    return {
      session,
      setHandlers: (h: any) => {
        handlers = h;
      },
      getHandlers: () => handlers,
      reset: () => {
        handlers = null;
        session.appendAudio.mockClear();
        session.cancelResponse.mockClear();
        session.truncateOutputAudio.mockClear();
        session.close.mockClear();
      },
    };
  });

  const envSnapshot = {
    OPENAI_API_KEY: process.env.OPENAI_API_KEY,
    ELEVENLABS_API_KEY: process.env.ELEVENLABS_API_KEY,
    ELEVENLABS_TTS_VOICE_ID: process.env.ELEVENLABS_TTS_VOICE_ID,
    VOICE_SYSTEM_PROMPT: process.env.VOICE_SYSTEM_PROMPT,
    VOICE_MEMORY_PATH: process.env.VOICE_MEMORY_PATH,
  };

  beforeEach(() => {
    process.env.OPENAI_API_KEY = 'test-openai';
    process.env.ELEVENLABS_API_KEY = 'test-elevenlabs';
    process.env.ELEVENLABS_TTS_VOICE_ID = 'voice-123';
  });

  vi.mock('../config.js', () => ({
    loadConfig: vi.fn().mockResolvedValue({
      audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
      normalization: {},
      storage: { driver: 'jsonl', path: './runs' },
      providers: ['elevenlabs'],
      jobs: { maxParallel: 4, retentionMs: 10 * 60 * 1000, retry: { maxAttempts: 3, baseDelayMs: 1000, maxDelayMs: 10_000 } },
      voice: {
        tts: { strategy: 'stream' },
        meeting: {
          introEnabled: false,
          openWindowMs: 6000,
          cooldownMs: 1500,
          echoSuppressMs: 3000,
          echoSimilarity: 0.8,
        },
      },
      ws: { keepaliveMs: 0, maxMissedPongs: 0 },
      providerHealth: {},
      providerLimits: {},
    }),
  }));

  vi.mock('../adapters/index.js', () => ({
    getAdapter: vi.fn(() => ({
      id: 'elevenlabs',
      supportsStreaming: true,
      supportsBatch: true,
      startStreaming: vi.fn(async () => ({
        controller: state.controller,
        onData: (cb: (t: any) => void) => state.setOnData(cb),
        onError: () => {},
        onClose: () => {},
      })),
      transcribeFileFromPCM: vi.fn(),
    })),
  }));

  vi.mock('../voice/openaiResponses.js', () => ({
    generateChatReply: state.generateChatReply,
    getOpenAiResponsesUrl: () => 'https://api.openai.com/v1/responses',
  }));

  vi.mock('../voice/openaiRealtimeVoice.js', () => ({
    startOpenAiRealtimeVoiceSession: vi.fn((_opts: unknown, handlers: unknown) => {
      realtimeState.setHandlers(handlers);
      return realtimeState.session;
    }),
  }));

  vi.mock('../voice/elevenlabsTts.js', () => ({
    streamTtsPcm: vi.fn(async function* (_text: string, opts: { signal?: AbortSignal }) {
      yield Buffer.from([1, 2, 3, 4]);
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, 1000);
        opts.signal?.addEventListener('abort', () => {
          clearTimeout(t);
          resolve();
        });
      });
      if (opts.signal?.aborted) return;
      yield Buffer.from([5, 6, 7, 8]);
    }),
  }));

  const restoreEnv = (key: string, value: string | undefined) => {
    if (value === undefined) {
      delete process.env[key];
      return;
    }
    process.env[key] = value;
  };

  afterEach(() => {
    restoreEnv('OPENAI_API_KEY', envSnapshot.OPENAI_API_KEY);
    restoreEnv('ELEVENLABS_API_KEY', envSnapshot.ELEVENLABS_API_KEY);
    restoreEnv('ELEVENLABS_TTS_VOICE_ID', envSnapshot.ELEVENLABS_TTS_VOICE_ID);
    restoreEnv('VOICE_SYSTEM_PROMPT', envSnapshot.VOICE_SYSTEM_PROMPT);
    restoreEnv('VOICE_MEMORY_PATH', envSnapshot.VOICE_MEMORY_PATH);
    state.reset();
    realtimeState.reset();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  const parseJsonMessages = (ws: FakeWebSocket) =>
    ws.sent
      .filter((m): m is string => typeof m === 'string')
      .map((m) => JSON.parse(m));

  const flushMicrotasks = async (max = 10) => {
    for (let i = 0; i < max; i += 1) {
      await Promise.resolve();
    }
  };

  const waitForCondition = async (check: () => boolean, max = 25) => {
    for (let i = 0; i < max && !check(); i += 1) {
      await flushMicrotasks();
    }
  };

  it('starts session and produces assistant audio for a committed transcript', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit('message', Buffer.from(JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, options: { finalizeDelayMs: 0 } })), false);

    await waitForCondition(() => Boolean(state.getOnData()));

    state.getOnData()?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    expect(json.some((m) => m.type === 'voice_session')).toBe(true);
    expect(json.some((m) => m.type === 'voice_user_transcript' && m.isFinal === true)).toBe(true);
    expect(json.some((m) => m.type === 'voice_assistant_text' && m.isFinal === true)).toBe(true);
    expect(json.some((m) => m.type === 'voice_assistant_audio_start')).toBe(true);
    expect(json.some((m) => m.type === 'voice_assistant_audio_end' && m.reason === 'completed')).toBe(true);
    const audioStart = json.find((m) => m.type === 'voice_assistant_audio_start');
    expect(typeof audioStart?.llmMs).toBe('number');
    expect(typeof audioStart?.ttsTtfbMs).toBe('number');

    const binaries = ws.sent.filter((m): m is Buffer => Buffer.isBuffer(m));
    expect(binaries.length).toBeGreaterThanOrEqual(1);
  });

  it('appends memory file contents to the system prompt', async () => {
    vi.useFakeTimers();
    process.env.VOICE_SYSTEM_PROMPT = 'Base prompt';
    process.env.VOICE_MEMORY_PATH = '/tmp/voice-memory.txt';

    vi.doMock('node:fs/promises', async () => {
      const actual = await vi.importActual<typeof FsPromises>('node:fs/promises');
      return {
        ...actual,
        readFile: vi.fn(async () => 'Memory line'),
      };
    });
    vi.resetModules();

    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    try {
      await handleVoiceConnection(ws as any, 'ja-JP');
      ws.emit(
        'message',
        Buffer.from(
          JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, options: { finalizeDelayMs: 0 } })
        ),
        false
      );

      await waitForCondition(() => Boolean(state.getOnData()));

      state.getOnData()?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
      vi.runOnlyPendingTimers();
      await flushMicrotasks();

      const snapshots = state.getChatSnapshots();
      expect(snapshots.length).toBeGreaterThan(0);
      const first = snapshots[0] as Array<{ role: string; content: string }>;
      expect(first[0]?.role).toBe('system');
      expect(first[0]?.content).toContain('Base prompt');
      expect(first[0]?.content).toContain('Memory line');
    } finally {
      vi.unmock('node:fs/promises');
      vi.resetModules();
    }
  });

  it('uses 24kHz output when OpenAI STT is selected (pipeline mode)', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 48000, presetId: 'openai' })),
      false
    );

    await waitForCondition(() => parseJsonMessages(ws).some((m) => m.type === 'voice_session'));

    const json = parseJsonMessages(ws);
    const sessionMsg = json.find((m) => m.type === 'voice_session');
    expect(sessionMsg).toBeDefined();
    expect(sessionMsg.outputAudioSpec?.sampleRate).toBe(24000);

    const adapters = await import('../adapters/index.js');
    const getAdapterMock = adapters.getAdapter as unknown as { mock: { results: Array<{ value: any }> } };
    const adapterInstance = getAdapterMock.mock.results.at(-1)?.value as { startStreaming?: any } | undefined;
    expect(adapterInstance?.startStreaming).toBeDefined();
    expect(adapterInstance?.startStreaming).toHaveBeenCalledWith(expect.objectContaining({ sampleRateHz: 24000 }));

    ws.close();
  });

  it('supports meeting channelSplit by starting two STT sessions', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: { finalizeDelayMs: 0, meetingMode: true, meetingRequireWakeWord: false },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);

    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[0]?.({ isFinal: true, text: 'マイク側の発話', provider: 'elevenlabs' });
    handlers[1]?.({ isFinal: true, text: '会議側の発話', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    const finals = json.filter((m) => m.type === 'voice_user_transcript' && m.isFinal === true);
    expect(finals.some((m) => m.source === 'mic')).toBe(true);
    expect(finals.some((m) => m.source === 'meeting')).toBe(true);
  });

  it('does not trigger a reply from meeting transcripts without wake words (barge-in)', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: { finalizeDelayMs: 0, meetingMode: true },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[0]?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();

    for (let i = 0; i < 10; i += 1) {
      await flushMicrotasks();
      const json = parseJsonMessages(ws);
      if (json.some((m) => m.type === 'voice_assistant_audio_start')) break;
      vi.advanceTimersByTime(50);
    }

    expect(state.generateChatReply).toHaveBeenCalledTimes(1);

    handlers[1]?.({ isFinal: true, text: '会議側の発話', provider: 'elevenlabs' });
    ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'barge_in' })), false);
    vi.runOnlyPendingTimers();
    await flushMicrotasks();
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();

    expect(state.generateChatReply).toHaveBeenCalledTimes(1);

    const json = parseJsonMessages(ws);
    const meetingFinal = json.find(
      (m) => m.type === 'voice_user_transcript' && m.isFinal === true && m.source === 'meeting'
    ) as { triggered?: boolean } | undefined;
    expect(meetingFinal).toBeDefined();
    expect(meetingFinal?.triggered).toBe(false);
  });

  it('does not match wake words inside other words in meeting mode', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'en-US');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: { finalizeDelayMs: 0, meetingMode: true },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    // "said" contains "ai" but should not be treated as a wake word.
    handlers[1]?.({ isFinal: true, text: 'said', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    expect(state.generateChatReply).toHaveBeenCalledTimes(0);

    // Actual wake-word usage should still trigger a reply (word boundary match).
    handlers[1]?.({ isFinal: true, text: 'AI, hello', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();

    expect(state.generateChatReply).toHaveBeenCalledTimes(1);
  });

  it('does not trigger a reply on wake word only', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: { finalizeDelayMs: 0, meetingMode: true, meetingRequireWakeWord: true },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[1]?.({ isFinal: true, text: 'アシスタント', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    expect(state.generateChatReply).toHaveBeenCalledTimes(0);

    const json = parseJsonMessages(ws);
    const meetingFinal = json.find(
      (m) => m.type === 'voice_user_transcript' && m.isFinal === true && m.source === 'meeting' && m.text === 'アシスタント'
    ) as { triggered?: boolean } | undefined;
    expect(meetingFinal?.triggered).toBe(false);
    expect(json.some((m) => m.type === 'voice_meeting_window' && m.state === 'opened')).toBe(true);
  });

  it('allows repeated wake words when open window is disabled', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: {
            finalizeDelayMs: 0,
            meetingMode: true,
            meetingRequireWakeWord: true,
            meetingOpenWindowMs: 0,
            meetingCooldownMs: 5000,
          },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[1]?.({ isFinal: true, text: 'アシスタント、最初の質問', provider: 'elevenlabs' });
    handlers[1]?.({ isFinal: true, text: 'アシスタント、次の質問', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    const first = json.find(
      (m) =>
        m.type === 'voice_user_transcript'
        && m.isFinal === true
        && m.source === 'meeting'
        && m.text === 'アシスタント、最初の質問'
    ) as { triggered?: boolean } | undefined;
    const second = json.find(
      (m) =>
        m.type === 'voice_user_transcript'
        && m.isFinal === true
        && m.source === 'meeting'
        && m.text === 'アシスタント、次の質問'
    ) as { triggered?: boolean } | undefined;
    expect(first?.triggered).toBe(true);
    expect(second?.triggered).toBe(true);
  });

  it('opens a meeting window after a wake word and triggers follow-ups', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: {
            finalizeDelayMs: 0,
            meetingMode: true,
            meetingRequireWakeWord: true,
            meetingOpenWindowMs: 5000,
          },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[1]?.({ isFinal: true, text: 'アシスタント、最初の質問', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    handlers[1]?.({ isFinal: true, text: '続きの質問', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    const follow = json.find(
      (m) => m.type === 'voice_user_transcript' && m.isFinal === true && m.source === 'meeting' && m.text === '続きの質問'
    ) as { triggered?: boolean } | undefined;
    expect(follow?.triggered).toBe(true);
    expect(json.some((m) => m.type === 'voice_meeting_window' && m.state === 'opened')).toBe(true);
  });

  it('suppresses meeting transcripts that echo assistant output', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: {
            finalizeDelayMs: 0,
            meetingMode: true,
            meetingRequireWakeWord: false,
            meetingOutputEnabled: true,
          },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[0]?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();

    handlers[1]?.({ isFinal: true, text: '了解しました。', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    const echoed = json.find(
      (m) =>
        m.type === 'voice_user_transcript'
        && m.isFinal === true
        && m.source === 'meeting'
        && m.text === '了解しました。'
    );
    expect(echoed).toBeUndefined();
  });

  it('preserves meeting system prompt across reset_history', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({
          type: 'config',
          pcm: true,
          clientSampleRate: 16000,
          channels: 2,
          channelSplit: true,
          options: { finalizeDelayMs: 0, meetingMode: true },
        })
      ),
      false
    );

    await waitForCondition(() => state.getOnDataAll().length >= 2);
    const handlers = state.getOnDataAll();
    expect(handlers.length).toBe(2);

    handlers[0]?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'reset_history' })), false);
    await flushMicrotasks();

    handlers[0]?.({ isFinal: true, text: '次の発話', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    const snapshots = state.getChatSnapshots();
    expect(snapshots.length).toBeGreaterThanOrEqual(2);
    const first = snapshots[0] as Array<{ role: string; content: string }>;
    const second = snapshots[1] as Array<{ role: string; content: string }>;
    expect(first[0]?.role).toBe('system');
    expect(first[0]?.content).toContain('あなたはWeb会議に参加しています');
    expect(second[0]?.role).toBe('system');
    expect(second[0]?.content).toContain('あなたはWeb会議に参加しています');
  });

  it('supports barge-in by aborting assistant speech', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit('message', Buffer.from(JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, options: { finalizeDelayMs: 0 } })), false);

    await waitForCondition(() => Boolean(state.getOnData()));

    state.getOnData()?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    for (let i = 0; i < 5; i += 1) {
      await flushMicrotasks();
      const json = parseJsonMessages(ws);
      if (json.some((m) => m.type === 'voice_assistant_audio_start')) break;
    }

    ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'barge_in' })), false);
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    expect(json.some((m) => m.type === 'voice_assistant_audio_end' && m.reason === 'barge_in')).toBe(true);
  });

  it('resets history by stopping assistant speech and clearing context', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, options: { finalizeDelayMs: 0 } })),
      false
    );

    await waitForCondition(() => Boolean(state.getOnData()));

    state.getOnData()?.({ isFinal: true, text: 'こんにちは', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    for (let i = 0; i < 10; i += 1) {
      await flushMicrotasks();
      const json = parseJsonMessages(ws);
      if (json.some((m) => m.type === 'voice_assistant_audio_start')) break;
    }

    const beforeResetBinaries = ws.sent.filter((m): m is Buffer => Buffer.isBuffer(m));
    expect(beforeResetBinaries.length).toBe(1);

    ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'reset_history' })), false);
    vi.advanceTimersByTime(1000);
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    expect(json.some((m) => m.type === 'voice_assistant_audio_end' && m.reason === 'stopped')).toBe(true);

    const afterResetBinaries = ws.sent.filter((m): m is Buffer => Buffer.isBuffer(m));
    expect(afterResetBinaries.length).toBe(1);

    state.getOnData()?.({ isFinal: true, text: '次の発話', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    const snapshots = state.getChatSnapshots();
    expect(snapshots.length).toBeGreaterThanOrEqual(2);
    const second = snapshots[1] as Array<{ role: string; content: string }>;
    expect(second.length).toBe(2); // system + current user
    expect(second[1]?.role).toBe('user');
    expect(second[1]?.content).toBe('次の発話');
  });

  it('aborts thinking turns without clobbering the next assistant turn', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    let rejectFirst: ((err: Error) => void) | null = null;
    const firstChat = new Promise<{ text: string; citations: any[] }>((_resolve, reject) => {
      rejectFirst = reject;
    });
    state.setChatQueue([() => firstChat, async () => ({ text: '次の返答です。', citations: [] })]);

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(
        JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, options: { finalizeDelayMs: 0 } })
      ),
      false
    );

    await waitForCondition(() => Boolean(state.getOnData()));

    // Start the first turn -> it gets stuck "thinking" waiting on firstChat.
    state.getOnData()?.({ isFinal: true, text: '最初の質問', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    // While the assistant is thinking, user speaks again; this should abort the in-flight turn and start a new one.
    state.getOnData()?.({ isFinal: true, text: '追加の質問', provider: 'elevenlabs' });
    vi.runOnlyPendingTimers();
    await flushMicrotasks();

    for (let i = 0; i < 10; i += 1) {
      await flushMicrotasks();
      const json = parseJsonMessages(ws);
      if (json.some((m) => m.type === 'voice_assistant_audio_start')) break;
    }

    // Resolve the first (aborted) LLM call after the second turn has already started.
    (rejectFirst as ((err: Error) => void) | null)?.(new Error('AbortError'));
    await flushMicrotasks();

    // Stop the second assistant speech; this must still work even if the first turn completes late.
    ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'stop_speaking' })), false);
    await flushMicrotasks();

    const json = parseJsonMessages(ws);
    expect(json.some((m) => m.type === 'voice_assistant_audio_start')).toBe(true);
    expect(json.some((m) => m.type === 'voice_assistant_audio_end' && m.reason === 'stopped')).toBe(true);

    const signals = state.getChatSignals();
    expect(signals.length).toBeGreaterThanOrEqual(2);
    expect(signals[0]?.aborted).toBe(true);
  });

  it('trims history to the configured max turns', async () => {
    vi.useFakeTimers();
    const prev = process.env.VOICE_HISTORY_MAX_TURNS;
    process.env.VOICE_HISTORY_MAX_TURNS = '1';
    vi.resetModules();
    try {
      const { handleVoiceConnection } = await import('./voiceHandler.js');
      const ws = new FakeWebSocket();

      await handleVoiceConnection(ws as any, 'ja-JP');
      ws.emit(
        'message',
        Buffer.from(
          JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, options: { finalizeDelayMs: 0 } })
        ),
        false
      );

      await waitForCondition(() => Boolean(state.getOnData()));

      state.getOnData()?.({ isFinal: true, text: '最初の発話', provider: 'elevenlabs' });
      vi.runOnlyPendingTimers();
      for (let i = 0; i < 10; i += 1) {
        await flushMicrotasks();
        const json = parseJsonMessages(ws);
        if (json.some((m) => m.type === 'voice_assistant_audio_start')) break;
      }

      ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'stop_speaking' })), false);
      await flushMicrotasks();

      state.getOnData()?.({ isFinal: true, text: '次の発話', provider: 'elevenlabs' });
      vi.runOnlyPendingTimers();
      await flushMicrotasks();

      const snapshots = state.getChatSnapshots();
      expect(snapshots.length).toBeGreaterThanOrEqual(2);
      expect(Array.isArray(snapshots[1])).toBe(true);
      const second = snapshots[1] as Array<{ role: string; content: string }>;
      expect(second.length).toBe(2); // system + current user
      expect(second[1]?.role).toBe('user');
      expect(second[1]?.content).toBe('次の発話');
    } finally {
      if (prev === undefined) {
        delete process.env.VOICE_HISTORY_MAX_TURNS;
      } else {
        process.env.VOICE_HISTORY_MAX_TURNS = prev;
      }
    }
  });

  it('ignores OpenAI Realtime audio deltas for mismatched responseId', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, presetId: 'openai_realtime' })),
      false
    );

    for (let i = 0; i < 10 && !realtimeState.getHandlers(); i += 1) {
      await Promise.resolve();
    }

    const handlers = realtimeState.getHandlers();
    expect(handlers).toBeTruthy();

    handlers.onResponseCreated?.({ responseId: 'r1' });
    handlers.onAssistantAudioDelta?.({ responseId: 'r2', itemId: 'item2', pcm: Buffer.from([1, 2, 3, 4]) });
    await flushMicrotasks();

    const binaries = ws.sent.filter((m): m is Buffer => Buffer.isBuffer(m));
    expect(binaries.length).toBe(0);

    const json = parseJsonMessages(ws);
    expect(json.some((m) => m.type === 'voice_assistant_audio_start')).toBe(false);
  });

  it('uses playedMs to truncate OpenAI Realtime output on stop_speaking', async () => {
    vi.useFakeTimers();
    const { handleVoiceConnection } = await import('./voiceHandler.js');
    const ws = new FakeWebSocket();

    await handleVoiceConnection(ws as any, 'ja-JP');
    ws.emit(
      'message',
      Buffer.from(JSON.stringify({ type: 'config', pcm: true, clientSampleRate: 16000, presetId: 'openai_realtime' })),
      false
    );

    for (let i = 0; i < 10 && !realtimeState.getHandlers(); i += 1) {
      await Promise.resolve();
    }

    const handlers = realtimeState.getHandlers();
    expect(handlers).toBeTruthy();

    handlers.onResponseCreated?.({ responseId: 'r1' });
    handlers.onAssistantAudioDelta?.({ responseId: 'r1', itemId: 'item1', pcm: Buffer.alloc(4800) }); // ~100ms @ 24kHz mono PCM16
    await flushMicrotasks();

    ws.emit('message', Buffer.from(JSON.stringify({ type: 'command', name: 'stop_speaking', playedMs: 50 })), false);
    await flushMicrotasks();

    expect(realtimeState.session.truncateOutputAudio).toHaveBeenCalledWith('item1', 50);
    expect(realtimeState.session.cancelResponse).toHaveBeenCalled();

    const json = parseJsonMessages(ws);
    expect(json.some((m) => m.type === 'voice_assistant_audio_end' && m.reason === 'stopped')).toBe(true);
  });
});
