export interface MongoSourceBenchmarkConfiguration {
  readonly sourceUrl: string;
}

export function resolveMongoSourceBenchmarkConfiguration(
  environment: Readonly<Record<string, string | undefined>> = process.env
): MongoSourceBenchmarkConfiguration {
  const sourceUrl = requiredEnvironmentUrl(environment, 'BENCHMARK_MONGODB_SOURCE_URL');
  canonicalMongoAuthority(sourceUrl);
  return { sourceUrl };
}

export function assertDistinctMongoSourceAndStorage(environment: Readonly<Record<string, string | undefined>>): void {
  const sourceUrl = requiredEnvironmentUrl(environment, 'BENCHMARK_MONGODB_SOURCE_URL');
  const storageUrl = requiredEnvironmentUrl(environment, 'BENCHMARK_MONGODB_STORAGE_URL');
  if (canonicalMongoAuthority(sourceUrl) === canonicalMongoAuthority(storageUrl)) {
    throw new Error('MongoDB source and storage must use distinct server authorities');
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
    .map((host) => canonicalMongoHost(host.trim().toLowerCase(), scheme))
    .filter((host) => host.length > 0)
    .sort();
  if (hosts.length === 0) throw new Error('MongoDB benchmark URL must contain at least one host');
  return `${scheme}://${hosts.join(',')}`;
}

function canonicalMongoHost(host: string, scheme: string): string {
  if (host.length === 0 || scheme === 'mongodb+srv') return host;
  if (host.startsWith('[')) {
    const closingBracket = host.indexOf(']');
    if (closingBracket === -1) throw new Error('MongoDB benchmark URL contains an invalid IPv6 host');
    return closingBracket === host.length - 1 ? `${host}:27017` : host;
  }
  return /:\d+$/.test(host) ? host : `${host}:27017`;
}

function requiredEnvironmentUrl(environment: Readonly<Record<string, string | undefined>>, variable: string): string {
  const value = environment[variable]?.trim();
  if (value == null || value.length === 0) throw new Error(`${variable} is required for MongoDB source benchmarks`);
  return value;
}
