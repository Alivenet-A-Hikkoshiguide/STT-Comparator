import { Readable } from 'node:stream';
import type {
  BatchResult,
  ProviderAdapter,
  ProviderId,
  StreamingOptions,
  StreamingSession,
} from '../types.js';

export abstract class BaseAdapter implements ProviderAdapter {
  abstract id: ProviderId;
  abstract supportsStreaming: boolean;
  abstract supportsBatch: boolean;

  abstract startStreaming(opts: StreamingOptions): Promise<StreamingSession>;

  abstract transcribeFileFromPCM(
    pcm: NodeJS.ReadableStream,
    opts: StreamingOptions
  ): Promise<BatchResult>;

  transcribePcmBuffer(pcm: Buffer, opts: StreamingOptions): Promise<BatchResult> {
    return this.transcribeFileFromPCM(Readable.from(pcm), opts);
  }
}
