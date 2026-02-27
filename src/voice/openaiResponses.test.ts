import { afterEach, describe, expect, it, vi } from 'vitest';

const OPENAI_RESPONSES_DEFAULT_URL = 'https://api.openai.com/v1/responses';

const envSnapshot = {
  OPENAI_API_KEY: process.env.OPENAI_API_KEY,
  OPENAI_RESPONSES_URL: process.env.OPENAI_RESPONSES_URL,
  OPENAI_CHAT_URL: process.env.OPENAI_CHAT_URL,
  OPENAI_RESPONSES_TIMEOUT_MS: process.env.OPENAI_RESPONSES_TIMEOUT_MS,
  OPENAI_WEB_SEARCH_ENABLED: process.env.OPENAI_WEB_SEARCH_ENABLED,
};

const restoreEnv = (key: keyof typeof envSnapshot) => {
  const value = envSnapshot[key];
  if (value === undefined) {
    delete process.env[key];
  } else {
    process.env[key] = value;
  }
};

afterEach(() => {
  restoreEnv('OPENAI_API_KEY');
  restoreEnv('OPENAI_RESPONSES_URL');
  restoreEnv('OPENAI_CHAT_URL');
  restoreEnv('OPENAI_RESPONSES_TIMEOUT_MS');
  restoreEnv('OPENAI_WEB_SEARCH_ENABLED');
  vi.unstubAllGlobals();
  vi.useRealTimers();
  vi.resetModules();
});

describe('getOpenAiResponsesUrl', () => {
  it('returns the default OpenAI responses URL when unset', async () => {
    delete process.env.OPENAI_RESPONSES_URL;
    delete process.env.OPENAI_CHAT_URL;
    vi.resetModules();
    const { getOpenAiResponsesUrl } = await import('./openaiResponses.js');
    expect(getOpenAiResponsesUrl()).toBe(OPENAI_RESPONSES_DEFAULT_URL);
  });

  it('rejects non-https URLs', async () => {
    process.env.OPENAI_RESPONSES_URL = 'http://api.openai.com/v1/responses';
    vi.resetModules();
    const { getOpenAiResponsesUrl } = await import('./openaiResponses.js');
    expect(() => getOpenAiResponsesUrl()).toThrow(/https/i);
  });

  it('rejects disallowed hosts', async () => {
    process.env.OPENAI_RESPONSES_URL = 'https://example.com/v1/responses';
    vi.resetModules();
    const { getOpenAiResponsesUrl } = await import('./openaiResponses.js');
    expect(() => getOpenAiResponsesUrl()).toThrow(/host/i);
  });

  it('rejects deprecated chat completions URL', async () => {
    process.env.OPENAI_CHAT_URL = 'https://api.openai.com/v1/chat/completions';
    delete process.env.OPENAI_RESPONSES_URL;
    vi.resetModules();
    const { getOpenAiResponsesUrl } = await import('./openaiResponses.js');
    expect(() => getOpenAiResponsesUrl()).toThrow(/deprecated/i);
  });
});

describe('generateChatReply', () => {
  it('does not send metadata', async () => {
    process.env.OPENAI_API_KEY = 'test-key';
    process.env.OPENAI_WEB_SEARCH_ENABLED = 'false';
    delete process.env.OPENAI_RESPONSES_URL; // use default
    vi.resetModules();
    const { generateChatReply } = await import('./openaiResponses.js');

    const fetchMock = vi.fn(async (_url: unknown, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>;
      expect(body.metadata).toBeUndefined();
      expect(body.temperature).toBeUndefined();
      return {
        ok: true,
        json: async () => ({
          output: [
            {
              type: 'message',
              role: 'assistant',
              content: [{ type: 'output_text', text: 'ok' }],
            },
          ],
        }),
      } as any;
    });
    vi.stubGlobal('fetch', fetchMock as any);

    const reply = await generateChatReply([{ role: 'system', content: 'sys' }], { instructions: 'sys' });
    expect(reply.text).toBe('ok');
    expect(reply.citations).toEqual([]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const body = JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body ?? '{}')) as Record<string, unknown>;
    expect(body.instructions).toBe('sys');
    expect(body.input).toEqual([]);
  });

  it('extracts url citations from output text', async () => {
    process.env.OPENAI_API_KEY = 'test-key';
    process.env.OPENAI_WEB_SEARCH_ENABLED = 'false';
    vi.resetModules();
    const { generateChatReply } = await import('./openaiResponses.js');

    let capturedInit: RequestInit | undefined;
    const fetchMock = vi.fn(async (_url: unknown, init?: RequestInit) => {
      capturedInit = init;
      return {
      ok: true,
      json: async () => ({
        output: [
          {
            type: 'message',
            role: 'assistant',
            content: [
              {
                type: 'output_text',
                text: 'Hello world',
                annotations: [
                  {
                    type: 'url_citation',
                    url: 'https://example.com',
                    title: 'Example',
                    start_index: 0,
                    end_index: 5,
                  },
                ],
              },
            ],
          },
        ],
      }),
      };
    });
    vi.stubGlobal('fetch', fetchMock as any);

    const reply = await generateChatReply([{ role: 'user', content: 'hi' }], { instructions: 'sys' });
    expect(reply.text).toBe('Hello world');
    expect(reply.citations).toEqual([
      { url: 'https://example.com', title: 'Example', startIndex: 0, endIndex: 5 },
    ]);
    const body = JSON.parse(String(capturedInit?.body ?? '{}')) as Record<string, unknown>;
    expect(body.instructions).toBe('sys');
    expect(body.input).toEqual([{ role: 'user', content: 'hi' }]);
    expect(body.temperature).toBeUndefined();
  });

  it('times out when the request never completes', async () => {
    process.env.OPENAI_API_KEY = 'test-key';
    process.env.OPENAI_WEB_SEARCH_ENABLED = 'false';
    delete process.env.OPENAI_RESPONSES_URL; // use default
    process.env.OPENAI_RESPONSES_TIMEOUT_MS = '10';

    vi.useFakeTimers();
    vi.resetModules();
    const { generateChatReply } = await import('./openaiResponses.js');

    vi.stubGlobal(
      'fetch',
      vi.fn((_url: unknown, init?: RequestInit) => {
        const signal = init?.signal;
        return new Promise((_, reject) => {
          signal?.addEventListener(
            'abort',
            () => {
              const err = new Error('aborted');
              (err as any).name = 'AbortError';
              reject(err);
            },
            { once: true }
          );
        }) as any;
      })
    );

    const promise = generateChatReply(
      [
        { role: 'system', content: 'sys' },
        { role: 'user', content: 'hello' },
      ],
      { instructions: 'sys' }
    );

    vi.advanceTimersByTime(20);

    await expect(promise).rejects.toThrow(/timed out/i);
  });
});
