// Usage: pnpm exec node src/scripts/latency.ts source|storage|minio MILLISECONDS [JITTER]
const [dependency, delay, jitter = '0'] = process.argv.slice(2);
if (
  !['source', 'storage', 'minio'].includes(dependency) ||
  [delay, jitter].some((value) => value == null || !Number.isFinite(Number(value)) || Number(value) < 0)
) {
  throw new Error('Usage: latency.ts source|storage|minio MILLISECONDS [JITTER]');
}
const endpoint = process.env.BENCHMARK_PROXY_API ?? 'http://127.0.0.1:8474';
const url = `${endpoint}/proxies/${dependency}/toxics`;
const removed = await fetch(`${url}/benchmark_latency`, { method: 'DELETE' });
if (!removed.ok && removed.status !== 404) throw new Error(await removed.text());
if (Number(delay) > 0) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: 'benchmark_latency',
      type: 'latency',
      stream: 'downstream',
      toxicity: 1,
      attributes: { latency: Number(delay), jitter: Number(jitter) }
    })
  });
  if (!response.ok) throw new Error(await response.text());
}
console.log(`${dependency}: ${delay} ms downstream latency, ${jitter} ms jitter`);
