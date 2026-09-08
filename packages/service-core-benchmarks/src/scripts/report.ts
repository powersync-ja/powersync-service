//@ts-nocheck
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const getArtifacts = async () => {
  const dir = './benchmark-artifacts/json';
  const files = (await readdir(dir)).filter((file) => file.endsWith('.json')).sort();

  const contentPromises = files.map(async (f) => {
    const fileBuffer = await readFile(`${dir}/${f}`, { encoding: 'utf8' });
    const json = JSON.parse(fileBuffer);
    return { ...json, artifact: f };
  });

  const fileContents = await Promise.all(contentPromises);
  return fileContents;
};

const convertToTable = (results) => {
  const mapped = results.map((result) => ({
    id: result.scenario.id,
    artifact: result.artifact,
    status: result.status,
    description: result.scenario.description,
    rows: result.scenario.workload.snapshot_row_count,
    buckets: result.scenario.expected_bucket_count,
    put_payload_bytes_mean: result.summary?.counters?.put_payload_bytes_mean?.median,
    implementation: result.environment.implementation,
    version: result.environment.storage_version,
    rows_per_second: result.summary?.counters?.rows_per_second?.median,
    logical_mib_per_second: result.summary?.counters?.logical_mib_per_second?.median,
    ...Object.values(result.summary?.boundaries ?? {})[0]
  }));
  return mapped;
};

//https://jsonic.io/guides/json-to-markdown
function escapeCell(val) {
  return String(val ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\n/g, ' ');
}

//https://jsonic.io/guides/json-to-markdown
function jsonToMarkdownTable(data) {
  if (!Array.isArray(data) || data.length === 0) return '';
  const headers = Object.keys(data[0]);
  const headerRow = '| ' + headers.map(escapeCell).join(' | ') + ' |';
  const separator = '| ' + headers.map(() => '---').join(' | ') + ' |';
  const rows = data.map((row) => '| ' + headers.map((h) => escapeCell(row[h])).join(' | ') + ' |');
  return [headerRow, separator, ...rows].join('\n');
}

const writeMarkdownTable = async (results) => {
  const markdown = results
    .map(
      (result) =>
        `### ${escapeCell(result.id)}\n\n${escapeCell(result.description)}\n\n${jsonToMarkdownTable([
          { Metric: 'Status', Value: result.status },
          { Metric: 'Median duration', Value: `${format(result.median)} ms` },
          { Metric: 'Throughput', Value: `${format(result.rows_per_second)} rows/s` },
          { Metric: 'Logical throughput', Value: `${format(result.logical_mib_per_second)} MiB/s` },
          { Metric: 'Buckets', Value: format(result.buckets) },
          { Metric: 'Mean PUT payload', Value: `${format(result.put_payload_bytes_mean)} bytes` }
        ])}`
    )
    .join('\n\n');
  await writeFile('./benchmark-artifacts/report/output.md', markdown, 'utf8');
  await writeFile('./benchmark-artifacts/report/output.json', JSON.stringify(results, null, 2), 'utf8');
};

function format(value) {
  return typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('en-US', { maximumFractionDigits: 2 })
    : '—';
}

function escapeHtml(value) {
  return String(value ?? '').replace(
    /[&<>"']/g,
    (character) =>
      ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      })[character]
  );
}

function statisticsTable(statistics, unit) {
  return `<div class="scroll"><table><thead><tr><th>Metric${unit ? ` (${unit})` : ''}</th><th>Median</th><th>Min</th><th>Max</th><th>p95</th><th>Samples</th></tr></thead><tbody>${Object.entries(
    statistics ?? {}
  )
    .map(
      ([name, values]) =>
        `<tr><th>${escapeHtml(name)}</th>${['median', 'min', 'max', 'p95', 'sample_count'].map((key) => `<td>${format(values[key])}</td>`).join('')}</tr>`
    )
    .join('')}</tbody></table></div>`;
}

function htmlReport(artifacts, summaries) {
  const cards = summaries
    .map((result, index) => {
      const artifact = artifacts[index];
      const metrics = [
        ['Rows / second', format(result.rows_per_second)],
        [
          artifact.environment.source === 'synthetic-raw-bson' ? 'Raw BSON MiB / second' : 'Logical MiB / second',
          format(result.logical_mib_per_second)
        ],
        ['Median duration', `${format(result.median)} ms`],
        ['User buckets', format(result.buckets)]
      ];
      if (artifact.environment.run_wall_ms != null)
        metrics.push(['Full run duration', `${format(artifact.environment.run_wall_ms)} ms`]);
      return `<article class="run" data-search="${escapeHtml([result.id, result.description, result.status, result.artifact].join(' ').toLowerCase())}">
      <header><h2>${escapeHtml(result.id)}</h2><span class="status ${result.status === 'passed' ? 'passed' : 'other'}">${escapeHtml(result.status)}</span></header>
      <p>${escapeHtml(result.description)}</p>
      <div class="metrics">${metrics.map(([label, value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join('')}</div>
      <p class="muted">${format(artifact.summary?.counters?.source_rows?.median ?? artifact.scenario.workload.row_count)} measured rows · ${format(result.rows)} seeded rows · ${format(artifact.summary?.successful_iterations)} successful / ${format(artifact.summary?.measured_iterations)} measured iterations · Mean PUT payload: ${format(result.put_payload_bytes_mean)} bytes</p>
      <details><summary>Timing and counters</summary>
        ${statisticsTable(artifact.summary?.boundaries, 'ms')}
        ${statisticsTable(artifact.summary?.counters, '')}
      </details>
      <details><summary>Configuration, resource measurements and diagnostics</summary><pre>${escapeHtml(JSON.stringify(artifact, null, 2))}</pre></details>
      <p class="muted">Artifact: <a href="../json/${escapeHtml(encodeURIComponent(result.artifact))}">${escapeHtml(result.artifact)}</a></p>
    </article>`;
    })
    .join('\n');
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>PowerSync benchmark results</title>
<style>
  :root { color-scheme: light dark; font: 16px/1.5 system-ui, sans-serif; background: light-dark(#f4f6f8, #13171d); color: light-dark(#18212d, #e1e7ef); }
  body { max-width: 1100px; margin: auto; padding: 24px; }
  h1 { margin-bottom: 4px; } h2 { font-size: 1rem; margin: 0; overflow-wrap: anywhere; }
  .muted, .metrics span { color: light-dark(#576575, #a6b3c3); font-size: .875rem; }
  input { box-sizing: border-box; width: 100%; padding: 12px; margin: 8px 0 20px; border: 1px solid #8090a0; border-radius: 6px; font: inherit; }
  article { background: light-dark(white, #1c232d); border: 1px solid light-dark(#dde3ea, #354253); border-radius: 10px; padding: 20px; margin-bottom: 18px; }
  header { display: flex; align-items: start; justify-content: space-between; gap: 16px; }
  .status { border-radius: 5px; padding: 2px 8px; font-size: .8rem; font-weight: 600; }
  .passed { background: #d6f5e4; color: #165537; } .other { background: #ffe6c7; color: #713b00; }
  .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 18px; margin: 20px 0; }
  .metrics span, .metrics strong { display: block; } .metrics strong { font-size: 1.4rem; font-variant-numeric: tabular-nums; }
  details { border-top: 1px solid light-dark(#dde3ea, #354253); padding: 12px 0; } summary { cursor: pointer; font-weight: 600; }
  .scroll { overflow-x: auto; } table { border-collapse: collapse; width: 100%; margin-top: 16px; font-size: .875rem; }
  th, td { padding: 8px; text-align: right; border-bottom: 1px solid light-dark(#e9edf2, #354253); font-variant-numeric: tabular-nums; }
  th:first-child { text-align: left; overflow-wrap: anywhere; } pre { white-space: pre-wrap; overflow-wrap: anywhere; font-size: .8rem; max-height: 60vh; overflow-y: auto; }
  a { color: light-dark(#1559a6, #89beff); overflow-wrap: anywhere; } [hidden] { display: none !important; }
</style></head><body>
<h1>PowerSync benchmark results</h1>
<p class="muted">Generated ${escapeHtml(new Date().toISOString())}. Warmups excluded from summary statistics. Compare runs with matching workloads and phases.</p>
<label for="filter">Filter by scenario, experiment label, or status</label>
<input id="filter" type="search" placeholder="e.g. catch-up or storage-20ms">
<p id="count" role="status">${summaries.length} runs</p>
${cards || '<p>No benchmark artifacts found. Run a benchmark first.</p>'}
<script>
  const filter = document.getElementById('filter');
  const runs = Array.from(document.querySelectorAll('.run'));
  filter.addEventListener('input', () => {
    const query = filter.value.toLowerCase();
    for (const run of runs) run.hidden = !run.dataset.search.includes(query);
    document.getElementById('count').textContent = runs.filter(run => !run.hidden).length + ' of ' + runs.length + ' runs';
  });
</script></body></html>`;
}

const main = async () => {
  const artifacts = await getArtifacts();
  const resultsTable = convertToTable(artifacts);
  await mkdir('./benchmark-artifacts/report', { recursive: true });
  await writeMarkdownTable(resultsTable);
  const htmlPath = resolve('./benchmark-artifacts/report/output.html');
  await writeFile(htmlPath, htmlReport(artifacts, resultsTable), 'utf8');
  for (const result of resultsTable) {
    console.log(`\n${result.status.toUpperCase()}  ${result.id}`);
    console.log(`  ${result.description}`);
    console.log(
      `  ${format(result.rows_per_second)} rows/s · ${format(result.logical_mib_per_second)} MiB/s · ${format(result.median)} ms median`
    );
  }
  console.log(`\nBrowser report: ${htmlPath}`);
  console.log('Also saved: benchmark-artifacts/report/output.md and output.json');
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
