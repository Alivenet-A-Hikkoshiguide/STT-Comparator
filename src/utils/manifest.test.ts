import { describe, expect, it } from 'vitest';
import type { EvaluationManifest } from '../types.js';
import { ManifestMatchError, matchManifestItem, validateManifestCoverage } from './manifest.js';

const manifest: EvaluationManifest = {
  version: 1,
  language: 'ja-JP',
  items: [
    { audio: 'samples/dirA/one.wav', ref: 'first' },
    { audio: 'samples/dirB/one.wav', ref: 'second' },
    { audio: 'simple.wav', ref: 'simple' },
  ],
};

describe('matchManifestItem', () => {
  it('matches when upload path and manifest path normalize exactly', () => {
    const item = matchManifestItem(manifest, 'samples/dirA/one.wav');
    expect(item?.ref).toBe('first');
  });

  it('matches windows-style separators by normalizing them first', () => {
    const item = matchManifestItem(manifest, 'samples\\dirB\\one.wav');
    expect(item?.ref).toBe('second');
  });

  it('falls back to basename by default when upload includes no directory info', () => {
    const item = matchManifestItem(
      {
        ...manifest,
        items: [{ audio: 'dataset/simple.wav', ref: 'simple' }],
      },
      'simple.wav'
    );
    expect(item?.ref).toBe('simple');
  });

  it('can disable basename fallback explicitly', () => {
    const item = matchManifestItem(
      {
        ...manifest,
        allowBasenameFallback: false,
        items: [{ audio: 'dataset/simple.wav', ref: 'simple' }],
      },
      'simple.wav'
    );
    expect(item).toBeUndefined();
  });

  it('throws on ambiguous basename fallback candidates', () => {
    expect(() => matchManifestItem(manifest, 'one.wav')).toThrow(ManifestMatchError);
  });

  it('returns undefined when there is no basename match', () => {
    expect(matchManifestItem(manifest, 'missing.wav')).toBeUndefined();
  });
});

describe('validateManifestCoverage', () => {
  it('reports missing and ambiguous files in one pass', () => {
    const result = validateManifestCoverage(manifest, ['one.wav', 'simple.wav', 'missing.wav']);
    expect(result.missingFiles).toEqual(['missing.wav']);
    expect(result.ambiguousFiles).toHaveLength(1);
    expect(result.ambiguousFiles[0]?.filename).toBe('one.wav');
    expect(result.ambiguousFiles[0]?.candidates).toEqual(['samples/dirA/one.wav', 'samples/dirB/one.wav']);
  });

  it('passes when all files resolve uniquely', () => {
    const result = validateManifestCoverage(
      { ...manifest, items: [{ audio: 'dataset/a.wav', ref: 'a' }, { audio: 'dataset/b.wav', ref: 'b' }] },
      ['a.wav', 'b.wav']
    );
    expect(result.missingFiles).toEqual([]);
    expect(result.ambiguousFiles).toEqual([]);
  });
});
