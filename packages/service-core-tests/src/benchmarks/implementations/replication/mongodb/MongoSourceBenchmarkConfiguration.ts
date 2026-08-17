import { StorageBenchmarkImplementationId } from '../../../types/StorageBenchmark.js';

export interface MongoSourceBenchmarkUrls {
  readonly sourceUrl: string;
  readonly storageUrl: string;
}

export function resolveMongoSourceBenchmarkConfiguration(
  storage: StorageBenchmarkImplementationId,
  environment: Readonly<Record<string, string | undefined>> = process.env
): MongoSourceBenchmarkUrls {
  const sourceUrl = requiredEnvironmentUrl(environment, 'BENCHMARK_MONGODB_SOURCE_URL');
  const storageVariable =
    storage === 'mongodb-storage' ? 'BENCHMARK_MONGODB_STORAGE_URL' : 'BENCHMARK_POSTGRES_STORAGE_URL';
  const storageUrl = requiredEnvironmentUrl(environment, storageVariable);
  const sourceAuthority = canonicalMongoAuthority(sourceUrl);

  if (storage === 'mongodb-storage') {
    if (sourceAuthority === canonicalMongoAuthority(storageUrl)) {
      throw new Error('MongoDB source and storage must use distinct server authorities');
    }
  } else {
    assertPostgresUrl(storageUrl, storageVariable);
  }

  return { sourceUrl, storageUrl };
}

function assertPostgresUrl(url: string, variable: string): void {
  try {
    const parsed = new URL(url);
    if (!['postgres:', 'postgresql:'].includes(parsed.protocol) || parsed.hostname.length === 0) throw new Error();
  } catch {
    throw new Error(`${variable} must be a valid PostgreSQL URL`);
  }
}

export function canonicalMongoAuthority(url: string): string {
  const match = /^(mongodb(?:\+srv)?):\/\/([^/?]+)(?:[/?]|$)/i.exec(url);
  if (match == null) throw new Error('MongoDB benchmark URL must be a valid mongodb:// or mongodb+srv:// URL');

  const scheme = match[1].toLowerCase();
  const authorityWithCredentials = match[2];
  const credentialSeparator = authorityWithCredentials.lastIndexOf('@');
  const authority =
    credentialSeparator === -1 ? authorityWithCredentials : authorityWithCredentials.slice(credentialSeparator + 1);
  const hosts = authority
    .split(',')
    .map((host) => host.trim().toLowerCase())
    .filter((host) => host.length > 0)
    .sort();
  if (hosts.length === 0) throw new Error('MongoDB benchmark URL must contain at least one host');
  return `${scheme}://${hosts.join(',')}`;
}

function requiredEnvironmentUrl(environment: Readonly<Record<string, string | undefined>>, variable: string): string {
  const value = environment[variable]?.trim();
  if (value == null || value.length === 0) throw new Error(`${variable} is required for MongoDB source benchmarks`);
  return value;
}
