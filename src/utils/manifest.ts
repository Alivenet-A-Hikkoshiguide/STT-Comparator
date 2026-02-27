import path from 'node:path';
import { z } from 'zod';
import type { EvaluationManifest } from '../types.js';

const manifestSchema = z.object({
  version: z.number().min(1),
  language: z.string(),
  items: z.array(
    z.object({
      audio: z.string(),
      ref: z.string(),
      meta: z.record(z.unknown()).optional(),
    })
  ),
  allowBasenameFallback: z.boolean().optional(),
  normalization: z
    .object({
      nfkc: z.boolean().optional(),
      stripPunct: z.boolean().optional(),
      stripSpace: z.boolean().optional(),
      lowercase: z.boolean().optional(),
  })
  .optional(),
});

export class ManifestMatchError extends Error {
  code: 'MANIFEST_AMBIGUOUS';
  details?: {
    filename?: string;
    basename?: string;
    candidates?: string[];
  };

  constructor(
    message: string,
    details?: {
      filename?: string;
      basename?: string;
      candidates?: string[];
    }
  ) {
    super(message);
    this.code = 'MANIFEST_AMBIGUOUS';
    this.details = details;
  }
}

function normalizeAudioPath(value: string): string {
  const raw = value.replace(/\\/g, '/');
  const normalized = path.posix.normalize(raw);
  const trimmed = normalized.replace(/^(\.\/)+/, '').replace(/^\/+/, '');
  return trimmed === '.' ? '' : trimmed;
}

export function parseManifest(json: string): EvaluationManifest {
  return manifestSchema.parse(JSON.parse(json));
}

function toBasename(value: string): string {
  return path.posix.basename(normalizeAudioPath(value) || value);
}

export function matchManifestItem(manifest: EvaluationManifest, filename: string) {
  const normalizedFilename = normalizeAudioPath(filename);
  const filenameBase = toBasename(filename);

  const exactMatch = manifest.items.find(
    (item) => normalizeAudioPath(item.audio) === normalizedFilename
  );
  if (exactMatch) {
    return exactMatch;
  }

  if (manifest.allowBasenameFallback === false) {
    return undefined;
  }

  const basenameMatches = manifest.items.filter((item) => {
    const itemBase = toBasename(item.audio);
    return itemBase === filenameBase;
  });

  if (basenameMatches.length > 1) {
    const candidates = basenameMatches
      .map((item) => normalizeAudioPath(item.audio))
      .filter((entry) => entry.length > 0);
    throw new ManifestMatchError(
      `ambiguous manifest match for "${filenameBase}" (${basenameMatches.length} candidates)`,
      {
        filename,
        basename: filenameBase,
        candidates,
      }
    );
  }

  return basenameMatches[0];
}

export interface ManifestCoverageValidation {
  missingFiles: string[];
  ambiguousFiles: Array<{
    filename: string;
    basename: string;
    candidates: string[];
  }>;
}

export function validateManifestCoverage(
  manifest: EvaluationManifest,
  filenames: readonly string[]
): ManifestCoverageValidation {
  const missingFiles: string[] = [];
  const ambiguousFiles: ManifestCoverageValidation['ambiguousFiles'] = [];

  for (const filename of filenames) {
    try {
      const matched = matchManifestItem(manifest, filename);
      if (!matched) {
        missingFiles.push(filename);
      }
    } catch (error) {
      if (error instanceof ManifestMatchError) {
        ambiguousFiles.push({
          filename,
          basename:
            typeof error.details?.basename === 'string'
              ? error.details.basename
              : toBasename(filename),
          candidates: Array.isArray(error.details?.candidates)
            ? error.details.candidates.filter((entry) => typeof entry === 'string')
            : [],
        });
        continue;
      }
      throw error;
    }
  }

  return { missingFiles, ambiguousFiles };
}
