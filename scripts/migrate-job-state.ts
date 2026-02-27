import path from 'node:path';
import { copyFile, mkdir, stat } from 'node:fs/promises';

interface CliArgs {
  from?: string;
  to?: string;
  force: boolean;
  help: boolean;
}

function printUsage(): void {
  // eslint-disable-next-line no-console
  console.log('Usage: tsx scripts/migrate-job-state.ts --from <legacy-db> --to <state-db> [--force]');
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { force: false, help: false };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--') {
      continue;
    }
    if (token === '--from') {
      args.from = argv[index + 1];
      index += 1;
      continue;
    }
    if (token === '--to') {
      args.to = argv[index + 1];
      index += 1;
      continue;
    }
    if (token === '--force' || token === '-f') {
      args.force = true;
      continue;
    }
    if (token === '--help' || token === '-h') {
      args.help = true;
      continue;
    }
    throw new Error(`unknown argument: ${token}`);
  }
  return args;
}

async function statIfNonEmpty(filePath: string): Promise<{ size: number } | null> {
  try {
    const file = await stat(filePath);
    if (!file.isFile() || file.size <= 0) {
      return null;
    }
    return { size: file.size };
  } catch {
    return null;
  }
}

async function copyDbWithCompanions(sourcePath: string, targetPath: string): Promise<void> {
  await mkdir(path.dirname(targetPath), { recursive: true });
  await copyFile(sourcePath, targetPath);

  for (const suffix of ['-wal', '-shm']) {
    const sourceCompanion = `${sourcePath}${suffix}`;
    const companion = await statIfNonEmpty(sourceCompanion);
    if (!companion) {
      continue;
    }
    await copyFile(sourceCompanion, `${targetPath}${suffix}`);
  }
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printUsage();
    return;
  }
  if (!args.from || !args.to) {
    throw new Error('--from and --to are required');
  }

  const sourcePath = path.resolve(args.from);
  const targetPath = path.resolve(args.to);

  if (sourcePath === targetPath) {
    throw new Error('--from and --to must point to different files');
  }

  const sourceInfo = await statIfNonEmpty(sourcePath);
  if (!sourceInfo) {
    throw new Error(`source database not found or empty: ${sourcePath}`);
  }

  const targetInfo = await statIfNonEmpty(targetPath);
  if (targetInfo && !args.force) {
    throw new Error(`target database already exists: ${targetPath} (use --force to overwrite)`);
  }

  await copyDbWithCompanions(sourcePath, targetPath);
  // eslint-disable-next-line no-console
  console.log(`migrated batch job state db: ${sourcePath} -> ${targetPath}`);
}

void main().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
