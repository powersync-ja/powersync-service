export interface PostgresSourceBenchmarkConfiguration {
  readonly sourceUrl: string;
}

export function resolvePostgresSourceBenchmarkConfiguration(
  environment: Readonly<Record<string, string | undefined>> = process.env
): PostgresSourceBenchmarkConfiguration {
  const sourceUrl = requiredEnvironmentUrl(
    environment,
    'BENCHMARK_POSTGRES_SOURCE_URL',
    'PostgreSQL source benchmarks'
  );
  canonicalPostgresAuthority(sourceUrl, 'BENCHMARK_POSTGRES_SOURCE_URL');
  return { sourceUrl };
}

export function assertDistinctPostgresSourceAndStorage(
  environment: Readonly<Record<string, string | undefined>>
): void {
  const sourceUrl = requiredEnvironmentUrl(
    environment,
    'BENCHMARK_POSTGRES_SOURCE_URL',
    'PostgreSQL source benchmarks'
  );
  const storageUrl = requiredEnvironmentUrl(
    environment,
    'BENCHMARK_POSTGRES_STORAGE_URL',
    'PostgreSQL storage benchmarks'
  );
  if (
    canonicalPostgresAuthority(sourceUrl, 'BENCHMARK_POSTGRES_SOURCE_URL') ===
    canonicalPostgresAuthority(storageUrl, 'BENCHMARK_POSTGRES_STORAGE_URL')
  ) {
    throw new Error('PostgreSQL source and storage must use distinct server authorities');
  }
}

export function canonicalPostgresAuthority(url: string, variable = 'PostgreSQL benchmark URL'): string {
  try {
    const parsed = new URL(url);
    if (!['postgres:', 'postgresql:'].includes(parsed.protocol) || parsed.hostname.length === 0) throw new Error();
    const port = parsed.port.length === 0 ? '5432' : parsed.port;
    return `postgresql://${parsed.hostname.toLowerCase()}:${port}`;
  } catch {
    throw new Error(`${variable} must be a valid postgres:// or postgresql:// URL`);
  }
}

function requiredEnvironmentUrl(
  environment: Readonly<Record<string, string | undefined>>,
  variable: string,
  description: string
): string {
  const value = environment[variable]?.trim();
  if (value == null || value.length === 0) throw new Error(`${variable} is required for ${description}`);
  return value;
}
