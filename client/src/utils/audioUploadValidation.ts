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

export const AUDIO_FILE_INPUT_ACCEPT = `audio/*,${ALLOWED_AUDIO_EXTENSIONS.join(',')}`;

function normalizeMimeType(value: string): string {
  return value.trim().toLowerCase();
}

function hasKnownMimeType(mimetype: string): boolean {
  return mimetype.length > 0 && mimetype !== 'application/octet-stream';
}

function isAudioMimeType(mimetype: string): boolean {
  return mimetype.startsWith('audio/') || EXTRA_AUDIO_MIME_TYPES.has(mimetype);
}

function getExtension(filename: string): string {
  const normalized = filename.toLowerCase();
  const dotIndex = normalized.lastIndexOf('.');
  return dotIndex >= 0 ? normalized.slice(dotIndex) : '';
}

function isAllowedExtension(filename: string): boolean {
  const ext = getExtension(filename);
  return ALLOWED_AUDIO_EXTENSIONS.includes(ext as (typeof ALLOWED_AUDIO_EXTENSIONS)[number]);
}

export function isLikelyAudioFile(file: Pick<File, 'name' | 'type'>): boolean {
  const mime = normalizeMimeType(file.type ?? '');
  const knownMime = hasKnownMimeType(mime);
  const audioMime = isAudioMimeType(mime);
  const audioExt = isAllowedExtension(file.name ?? '');

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

export function partitionAudioFiles(files: File[]): { accepted: File[]; rejected: File[] } {
  const accepted: File[] = [];
  const rejected: File[] = [];
  for (const file of files) {
    if (isLikelyAudioFile(file)) {
      accepted.push(file);
    } else {
      rejected.push(file);
    }
  }
  return { accepted, rejected };
}

export function formatRejectedFilesMessage(rejected: File[], maxNames = 3): string {
  const names = rejected.slice(0, maxNames).map((file) => file.name);
  const suffix = rejected.length > maxNames ? ` ほか${rejected.length - maxNames}件` : '';
  return `音声ファイル以外はアップロードできません: ${names.join(', ')}${suffix}`;
}
