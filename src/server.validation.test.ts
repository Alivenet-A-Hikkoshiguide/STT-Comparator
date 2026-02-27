import { describe, expect, it } from 'vitest';
import { parseBatchRequest, HttpError } from './server.js';

const availability = [
  { id: 'mock', available: true, implemented: true, supportsStreaming: true, supportsBatch: true },
  { id: 'deepgram', available: false, implemented: true, supportsStreaming: true, supportsBatch: true, reason: 'no key' },
] as const;

const baseReq = () =>
  ({
    files: [{ buffer: Buffer.from('a'), originalname: 'a.wav', mimetype: 'audio/wav', size: 1 }],
    body: { provider: 'mock', lang: 'ja-JP' },
  } as any);

const dummyConfig = {
  audio: { targetSampleRate: 16000, targetChannels: 1, chunkMs: 250 },
  normalization: {},
  storage: { driver: 'jsonl', path: './runs', retentionDays: 30, maxRows: 100000 },
  providers: ['mock'],
  providerLimits: { batchMaxBytes: {} },
} as any;

describe('parseBatchRequest', () => {
  it('ファイルなしは 400', () => {
    const req = baseReq();
    req.files = [] as any;
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
    try {
      parseBatchRequest(req as any, availability as any, dummyConfig);
    } catch (err) {
      expect((err as HttpError).statusCode).toBe(400);
      expect((err as Error).message).toMatch(/no files/);
    }
  });

  it('非音声ファイルは 400', () => {
    const req = baseReq();
    req.files = [
      {
        buffer: Buffer.from('not-audio'),
        originalname: 'report.wav.docx',
        mimetype: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        size: 1,
      },
    ] as any;
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
    try {
      parseBatchRequest(req as any, availability as any, dummyConfig);
    } catch (err) {
      const error = err as HttpError;
      expect(error.statusCode).toBe(400);
      expect(
        (error.payload?.invalidFiles as Array<{ originalname: string }> | undefined)?.[0]?.originalname
      ).toBe('report.wav.docx');
    }
  });

  it('壊れた manifest は 400', () => {
    const req = baseReq();
    req.body.ref_json = 'not-json';
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
  });

  it('options の JSON パース失敗で 400', () => {
    const req = baseReq();
    req.body.options = '{bad';
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
  });

  it('利用不可プロバイダは理由付きで 400', () => {
    const req = baseReq();
    req.body.provider = 'deepgram';
    try {
      parseBatchRequest(req as any, availability as any, dummyConfig);
    } catch (err) {
      expect((err as HttpError).statusCode).toBe(400);
      expect((err as Error).message).toMatch(/unavailable/);
    }
  });

  it('妥当な入力をパースできる', () => {
    const req = baseReq();
    req.body.options = JSON.stringify({ parallel: 2 });
    req.body.ref_json = JSON.stringify({ version: 1, language: 'ja-JP', items: [{ audio: 'a.wav', ref: 'hello' }] });
    const parsed = parseBatchRequest(req as any, availability as any, dummyConfig);
    expect(parsed.lang).toBe('ja-JP');
    expect(parsed.provider).toBe('mock');
    expect(parsed.manifest?.items[0].audio).toBe('a.wav');
    expect(parsed.options?.parallel).toBe(2);
  });

  it('manifest.language と lang の不一致は 400', () => {
    const req = baseReq();
    req.body.lang = 'en-US';
    req.body.ref_json = JSON.stringify({ version: 1, language: 'ja-JP', items: [{ audio: 'a.wav', ref: 'hello' }] });
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
    try {
      parseBatchRequest(req as any, availability as any, dummyConfig);
    } catch (err) {
      expect((err as HttpError).statusCode).toBe(400);
      expect((err as Error).message).toMatch(/lang mismatch/);
    }
  });

  it('manifest がある場合は manifest.language を優先する', () => {
    const req = baseReq();
    req.body.lang = 'ja';
    req.body.ref_json = JSON.stringify({ version: 1, language: 'ja-JP', items: [{ audio: 'a.wav', ref: 'hello' }] });
    const parsed = parseBatchRequest(req as any, availability as any, dummyConfig);
    expect(parsed.lang).toBe('ja-JP');
  });

  it('manifest の audio が相対パスでも basename が一意なら受理する', () => {
    const req = baseReq();
    req.body.ref_json = JSON.stringify({
      version: 1,
      language: 'ja-JP',
      items: [{ audio: 'dataset/ja/a.wav', ref: 'hello' }],
    });
    const parsed = parseBatchRequest(req as any, availability as any, dummyConfig);
    expect(parsed.manifest?.items[0]?.audio).toBe('dataset/ja/a.wav');
  });

  it('manifest basename が曖昧な場合は 400', () => {
    const req = baseReq();
    req.body.ref_json = JSON.stringify({
      version: 1,
      language: 'ja-JP',
      items: [
        { audio: 'dataset/a/a.wav', ref: 'first' },
        { audio: 'dataset/b/a.wav', ref: 'second' },
      ],
    });
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
    try {
      parseBatchRequest(req as any, availability as any, dummyConfig);
    } catch (err) {
      const error = err as HttpError;
      expect(error.statusCode).toBe(400);
      expect((error.payload?.ambiguousFiles as Array<{ filename: string }> | undefined)?.[0]?.filename).toBe('a.wav');
    }
  });

  it('manifest がアップロード全件をカバーしない場合は 400', () => {
    const req = baseReq();
    req.body.ref_json = JSON.stringify({
      version: 1,
      language: 'ja-JP',
      items: [{ audio: 'other.wav', ref: 'hello' }],
    });
    expect(() => parseBatchRequest(req as any, availability as any, dummyConfig)).toThrow(HttpError);
    try {
      parseBatchRequest(req as any, availability as any, dummyConfig);
    } catch (err) {
      const error = err as HttpError;
      expect(error.statusCode).toBe(400);
      expect(error.payload?.missingFiles).toEqual(['a.wav']);
    }
  });

  it('providerLimits.batchMaxBytes を超えるファイルは 413', () => {
    const req = baseReq();
    req.files = [{ buffer: Buffer.from('too-large'), originalname: 'a.wav', size: 12 }];
    const config = {
      ...dummyConfig,
      providerLimits: { batchMaxBytes: { mock: 10 } },
    };
    expect(() => parseBatchRequest(req as any, availability as any, config as any)).toThrow(HttpError);
    try {
      parseBatchRequest(req as any, availability as any, config as any);
    } catch (err) {
      expect((err as HttpError).statusCode).toBe(413);
      expect((err as Error).message).toMatch(/file too large/);
      expect((err as Error).message).toMatch(/mock/);
    }
  });
});
