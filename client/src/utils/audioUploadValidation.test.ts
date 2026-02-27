import { describe, expect, it } from 'vitest';
import {
  AUDIO_FILE_INPUT_ACCEPT,
  formatRejectedFilesMessage,
  isLikelyAudioFile,
  partitionAudioFiles,
} from './audioUploadValidation';

function makeFileLike(name: string, type: string): File {
  return { name, type } as File;
}

describe('audioUploadValidation', () => {
  it('accepts known audio files', () => {
    expect(isLikelyAudioFile(makeFileLike('voice.wav', 'audio/wav'))).toBe(true);
    expect(isLikelyAudioFile(makeFileLike('voice.webm', 'audio/webm'))).toBe(true);
  });

  it('rejects non-audio files', () => {
    expect(
      isLikelyAudioFile(
        makeFileLike(
          'report.wav.docx',
          'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
      )
    ).toBe(false);
  });

  it('splits accepted and rejected files', () => {
    const files = [
      makeFileLike('a.wav', 'audio/wav'),
      makeFileLike('b.mp3', 'audio/mpeg'),
      makeFileLike('c.docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'),
    ];
    const { accepted, rejected } = partitionAudioFiles(files);
    expect(accepted).toHaveLength(2);
    expect(rejected).toHaveLength(1);
  });

  it('formats rejected file names for UI', () => {
    const message = formatRejectedFilesMessage([
      makeFileLike('bad.docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'),
    ]);
    expect(message).toContain('bad.docx');
  });

  it('exposes file input accept pattern', () => {
    expect(AUDIO_FILE_INPUT_ACCEPT).toContain('audio/*');
    expect(AUDIO_FILE_INPUT_ACCEPT).toContain('.wav');
  });
});
