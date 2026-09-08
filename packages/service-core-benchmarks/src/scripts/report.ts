//@ts-nocheck
import { readdir, readFile, writeFile } from 'node:fs/promises';

const getArtifacts = async () => {
  const dir = './benchmark-artifacts/json';
  const files = await readdir(dir);

  const contentPromises = files.map(async (f) => {
    const fileBuffer = await readFile(`${dir}/${f}`, { encoding: 'utf8' });
    const json = JSON.parse(fileBuffer);
    return json;
  });

  const fileContents = await Promise.all(contentPromises);
  return fileContents;
};

const convertToTable = (results) => {
  const mapped = results.map((result) => ({
    id: result.scenario.id,
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
  const markdown = jsonToMarkdownTable(results);
  await writeFile('./benchmark-artifacts/report/output.md', markdown, 'utf8');
  await writeFile('./benchmark-artifacts/report/output.json', JSON.stringify(results, null, 2), 'utf8');
};

const main = async () => {
  const artifacts = await getArtifacts();
  const resultsTable = convertToTable(artifacts);
  await writeMarkdownTable(resultsTable);
  console.table(resultsTable);
};

main();
