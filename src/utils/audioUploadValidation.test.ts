import { describe, expect, it } from 'vitest';
import {
  findInvalidAudioUploads,
  getAllowedAudioExtensions,
  isLikelyAudioUpload,
} from './audioUploadValidation.js';

describe('audioUploadValidation', () => {
  it('accepts known audio extension and mime type', () => {
    expect(isLikelyAudioUpload({ originalname: 'sample.wav', mimetype: 'audio/wav' })).toBe(true);
  });

  it('accepts audio mime even when extension is missing', () => {
    expect(isLikelyAudioUpload({ originalname: 'recording', mimetype: 'audio/mpeg' })).toBe(true);
  });

  it('rejects non-audio document files', () => {
    const invalid = findInvalidAudioUploads([
      {
        originalname: '20260216172357-4004-0362309978.wav.docx',
        mimetype: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      },
    ]);
    expect(invalid).toHaveLength(1);
    expect(invalid[0]?.originalname).toContain('.docx');
  });

  it('rejects unknown extension without audio mime', () => {
    expect(isLikelyAudioUpload({ originalname: 'clip.bin', mimetype: 'application/octet-stream' })).toBe(false);
  });

  it('publishes allowed extensions for API error payload', () => {
    expect(getAllowedAudioExtensions()).toContain('.wav');
    expect(getAllowedAudioExtensions()).toContain('.mp3');
  });
});
