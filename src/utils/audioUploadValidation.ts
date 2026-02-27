import path from 'node:path';

const ALLOWED_AUDIO_EXTENSIONS = [
  '.wav',
  '.mp3',
  '.mp4',
  '.m4a',
  '.aac',
  '.flac',
  '.ogg',
  '.opus',
  '.webm',
] as const;

const EXTRA_AUDIO_MIME_TYPES = new Set([
  'application/ogg',
  'video/ogg',
  'video/webm',
  'video/mp4',
]);

export interface UploadFileLike {
  originalname?: string | null;
  mimetype?: string | null;
}

export interface InvalidAudioUpload {
  originalname: string;
  mimetype?: string;
  reason: 'unsupported_file_type';
}

export function getAllowedAudioExtensions(): readonly string[] {
  return ALLOWED_AUDIO_EXTENSIONS;
}

function normalizeMimeType(mimetype: string | null | undefined): string {
  return (mimetype ?? '').trim().toLowerCase();
}

function hasKnownMimeType(mimetype: string): boolean {
  return mimetype.length > 0 && mimetype !== 'application/octet-stream';
}

function isAudioMimeType(mimetype: string): boolean {
  return mimetype.startsWith('audio/') || EXTRA_AUDIO_MIME_TYPES.has(mimetype);
}

function isAllowedAudioExtension(filename: string): boolean {
  const ext = path.extname(filename).toLowerCase();
  return ALLOWED_AUDIO_EXTENSIONS.includes(ext as (typeof ALLOWED_AUDIO_EXTENSIONS)[number]);
}

export function isLikelyAudioUpload(file: UploadFileLike): boolean {
  const originalname = (file.originalname ?? '').trim();
  const mimetype = normalizeMimeType(file.mimetype);
  const knownMime = hasKnownMimeType(mimetype);
  const audioMime = isAudioMimeType(mimetype);
  const audioExt = isAllowedAudioExtension(originalname);

  if (knownMime && !audioMime) {
    return false;
  }

  if (audioExt) {
    return true;
  }

  if (audioMime) {
    return true;
  }

  return false;
}

export function findInvalidAudioUploads(files: UploadFileLike[]): InvalidAudioUpload[] {
  return files
    .filter((file) => !isLikelyAudioUpload(file))
    .map((file) => ({
      originalname: (file.originalname ?? '').trim() || '(unknown)',
      mimetype: normalizeMimeType(file.mimetype) || undefined,
      reason: 'unsupported_file_type' as const,
    }));
}
