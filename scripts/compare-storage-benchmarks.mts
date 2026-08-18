import inquirer from 'inquirer';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const OUTPUT_ENV = 'POWERSYNC_STORAGE_BENCHMARK_OUTPUT';

interface StorageBenchmarkResult {
  storage: string;
  version: number | null;
  scenario: string;
  source_rows: number;
  buckets: number;
  write_ms: number;
  write_rows_per_second: number;
  write_mebibytes_per_second: number;
  sync_drain_ms: number;
  ops: number;
  read_mebibytes_per_second: number;
}

interface StorageBenchmarkOutput {
  schema_version: 1;
  generated_at: string;
  results: StorageBenchmarkResult[];
}

interface StorageRunner {
  id: string;
  label: string;
  packageName: string;
  testName?: string;
}

const STORAGE_RUNNERS: StorageRunner[] = [
  {
    id: 'mongodb',
    label: 'MongoDB',
    packageName: '@powersync/service-module-mongodb-storage'
  },
  {
    id: 'postgresql',
    label: 'PostgreSQL',
    packageName: '@powersync/service-module-postgres-storage'
  },
  {
    id: 'drizzle-sqlite',
    label: 'Drizzle SQLite',
    packageName: '@powersync/service-module-drizzle-storage'
  },
  {
    id: 'mikroorm-sqlite',
    label: 'MikroORM SQLite',
    packageName: '@powersync/service-module-mikroorm-storage',
    testName: 'MikroORM SQLite'
  },
  {
    id: 'mikroorm-mysql',
    label: 'MikroORM MySQL',
    packageName: '@powersync/service-module-mikroorm-storage',
    testName: 'MikroORM MySQL'
  }
];

const STORAGE_ALIASES = new Map([
  ['mongo', 'mongodb'],
  ['postgres', 'postgresql'],
  ['drizzle:sqlite', 'drizzle-sqlite'],
  ['mikroorm:sqlite', 'mikroorm-sqlite'],
  ['mikroorm:mysql', 'mikroorm-mysql']
]);

interface CliOptions {
  storageIds: string[];
  outputPath?: string;
  chartPath: string;
  help: boolean;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const selectedIds = options.storageIds.length > 0 ? options.storageIds : await promptForStorageIds();
  const runners = selectRunners(selectedIds);
  const temporaryDirectory = await mkdtemp(join(tmpdir(), 'powersync-storage-benchmarks-'));
  const results: StorageBenchmarkResult[] = [];
  const failures: string[] = [];

  try {
    for (const runner of runners) {
      const resultPath = join(temporaryDirectory, `${runner.id}.json`);
      console.log(`\nRunning ${runner.label} storage benchmarks...\n`);

      const exitCode = await runStorageBenchmark(runner, resultPath);
      if (exitCode != 0) {
        failures.push(`${runner.label} (exit code ${exitCode})`);
        continue;
      }

      try {
        const output = parseBenchmarkOutput(await readFile(resultPath, 'utf8'), resultPath);
        results.push(...output.results);
      } catch (error) {
        failures.push(`${runner.label} (${error instanceof Error ? error.message : String(error)})`);
      }
    }

    if (results.length > 0) {
      console.log(`\nStorage benchmark comparison\n\n${formatComparison(results)}`);
      await writeComparisonChart(options.chartPath, results);
      console.log(`\nWrote benchmark comparison chart to ${options.chartPath}`);
    }

    if (options.outputPath != null) {
      const output: StorageBenchmarkOutput = {
        schema_version: 1,
        generated_at: new Date().toISOString(),
        results
      };
      await mkdir(dirname(options.outputPath), { recursive: true });
      await writeFile(options.outputPath, `${JSON.stringify(output, null, 2)}\n`, 'utf8');
      console.log(`\nWrote combined benchmark results to ${options.outputPath}`);
    }

    if (failures.length > 0) {
      console.error(`\nFailed benchmark runs:\n${failures.map((failure) => `- ${failure}`).join('\n')}`);
      process.exitCode = 1;
    }
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

function parseArgs(args: string[]): CliOptions {
  const storageIds: string[] = [];
  let outputPath: string | undefined;
  let chartPath = 'storage-benchmark-comparison.svg';
  let help = false;

  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument == '--help' || argument == '-h') {
      help = true;
    } else if (argument == '--storage' || argument == '-s') {
      storageIds.push(...readOptionValue(args, ++index, argument).split(','));
    } else if (argument.startsWith('--storage=')) {
      storageIds.push(...argument.slice('--storage='.length).split(','));
    } else if (argument == '--output' || argument == '-o') {
      outputPath = readOptionValue(args, ++index, argument);
    } else if (argument.startsWith('--output=')) {
      outputPath = argument.slice('--output='.length);
    } else if (argument == '--chart' || argument == '-c') {
      chartPath = readOptionValue(args, ++index, argument);
    } else if (argument.startsWith('--chart=')) {
      chartPath = argument.slice('--chart='.length);
    } else {
      throw new Error(`Unknown option: ${argument}`);
    }
  }

  return {
    storageIds: storageIds.map((id) => id.trim()).filter(Boolean),
    outputPath,
    chartPath,
    help
  };
}

function readOptionValue(args: string[], index: number, option: string) {
  const value = args[index];
  if (value == null || value.startsWith('-')) {
    throw new Error(`Missing value for ${option}`);
  }
  return value;
}

async function promptForStorageIds() {
  const answer = await inquirer.prompt<{ storageIds: string[] }>([
    {
      type: 'checkbox',
      name: 'storageIds',
      message: 'Which bucket storage implementations should be compared?',
      choices: STORAGE_RUNNERS.map((runner) => ({
        name: runner.label,
        value: runner.id,
        checked: runner.id == 'drizzle-sqlite' || runner.id == 'mikroorm-sqlite'
      })),
      validate: (selected: string[]) => selected.length > 0 || 'Select at least one storage implementation'
    }
  ]);
  return answer.storageIds;
}

function selectRunners(storageIds: string[]) {
  const normalizedIds = storageIds.flatMap((id) => {
    if (id == 'all') {
      return STORAGE_RUNNERS.map((runner) => runner.id);
    }
    return [STORAGE_ALIASES.get(id) ?? id];
  });
  const unknownIds = normalizedIds.filter((id) => !STORAGE_RUNNERS.some((runner) => runner.id == id));
  if (unknownIds.length > 0) {
    throw new Error(`Unknown storage implementation(s): ${[...new Set(unknownIds)].join(', ')}`);
  }

  return [...new Set(normalizedIds)].map((id) => STORAGE_RUNNERS.find((runner) => runner.id == id)!);
}

function runStorageBenchmark(runner: StorageRunner, resultPath: string) {
  const command = process.platform == 'win32' ? 'corepack.cmd' : 'corepack';
  const args = ['pnpm', '--filter', runner.packageName, 'test', 'test/src/storage_bench.test.ts', '--run'];
  if (runner.testName != null) {
    args.push('-t', runner.testName);
  }

  return new Promise<number>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: fileURLToPath(new URL('..', import.meta.url)),
      env: {
        ...process.env,
        [OUTPUT_ENV]: resultPath
      },
      stdio: 'inherit'
    });
    child.once('error', reject);
    child.once('close', (code, signal) => {
      if (signal != null) {
        console.error(`${runner.label} benchmark terminated by ${signal}`);
      }
      resolve(code ?? 1);
    });
  });
}

function parseBenchmarkOutput(json: string, source: string): StorageBenchmarkOutput {
  const parsed = JSON.parse(json) as Partial<StorageBenchmarkOutput>;
  if (parsed.schema_version != 1 || !Array.isArray(parsed.results)) {
    throw new Error(`Invalid benchmark output in ${source}`);
  }
  return parsed as StorageBenchmarkOutput;
}

function formatComparison(results: StorageBenchmarkResult[]) {
  const fastestWrites = maximumByScenario(results, (result) => result.write_mebibytes_per_second);
  const fastestDrains = maximumByScenario(results, (result) => result.read_mebibytes_per_second);
  const scenarioOrder = new Map<string, number>();
  for (const result of results) {
    if (!scenarioOrder.has(result.scenario)) {
      scenarioOrder.set(result.scenario, scenarioOrder.size);
    }
  }
  const rows = [
    '| Scenario | Storage | Version | Write rows/s | Write MiB/s | Write relative | Read MiB/s | Read relative | Write ms | Sync drain ms |',
    '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'
  ];

  for (const result of [...results].sort((left, right) => compareResults(left, right, scenarioOrder))) {
    rows.push(
      `| ${result.scenario} | ${result.storage} | ${result.version ?? ''} | ${formatNumber(
        result.write_rows_per_second
      )} | ${formatNumber(result.write_mebibytes_per_second)} | ${formatPercent(
        result.write_mebibytes_per_second,
        fastestWrites.get(result.scenario)!
      )} | ${formatNumber(result.read_mebibytes_per_second)} | ${formatPercent(
        result.read_mebibytes_per_second,
        fastestDrains.get(result.scenario)!
      )} | ${formatNumber(result.write_ms)} | ${formatNumber(result.sync_drain_ms)} |`
    );
  }
  return rows.join('\n');
}

function maximumByScenario(results: StorageBenchmarkResult[], value: (result: StorageBenchmarkResult) => number) {
  const maximums = new Map<string, number>();
  for (const result of results) {
    maximums.set(result.scenario, Math.max(maximums.get(result.scenario) ?? 0, value(result)));
  }
  return maximums;
}

function compareResults(
  left: StorageBenchmarkResult,
  right: StorageBenchmarkResult,
  scenarioOrder: Map<string, number>
) {
  return (
    scenarioOrder.get(left.scenario)! - scenarioOrder.get(right.scenario)! || left.storage.localeCompare(right.storage)
  );
}

function formatPercent(value: number, maximum: number) {
  return maximum == 0 ? 'n/a' : `${((value / maximum) * 100).toFixed(1)}%`;
}

function formatNumber(value: number) {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(value >= 100 ? 0 : 2);
}

export async function writeComparisonChart(outputPath: string, results: StorageBenchmarkResult[]) {
  const width = Math.max(960, results.length * 110 + 180);
  const height = 620;
  const margin = { top: 90, right: 40, bottom: 190, left: 90 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const maximum = niceCeiling(
    Math.max(...results.flatMap((result) => [result.write_mebibytes_per_second, result.read_mebibytes_per_second]))
  );
  const scenarioOrder = new Map<string, number>();
  for (const result of results) {
    if (!scenarioOrder.has(result.scenario)) {
      scenarioOrder.set(result.scenario, scenarioOrder.size);
    }
  }
  const orderedResults = [...results].sort((left, right) => compareResults(left, right, scenarioOrder));
  const groupWidth = plotWidth / orderedResults.length;
  const barWidth = Math.min(30, Math.max(8, groupWidth * 0.32));
  const barGap = Math.min(8, groupWidth * 0.08);
  const baseline = margin.top + plotHeight;
  const elements: string[] = [];

  for (let tick = 0; tick <= 5; tick++) {
    const value = (maximum * tick) / 5;
    const y = baseline - (plotHeight * tick) / 5;
    elements.push(
      `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="#d7dde5" stroke-width="1"/>`,
      `<text x="${margin.left - 12}" y="${y + 4}" text-anchor="end" class="tick">${escapeXml(
        formatNumber(value)
      )}</text>`
    );
  }

  orderedResults.forEach((result, index) => {
    const center = margin.left + groupWidth * (index + 0.5);
    const writeHeight = (result.write_mebibytes_per_second / maximum) * plotHeight;
    const readHeight = (result.read_mebibytes_per_second / maximum) * plotHeight;
    const writeX = center - barGap / 2 - barWidth;
    const readX = center + barGap / 2;
    const label = `${result.storage} v${result.version ?? 'default'} · ${result.scenario}`;
    elements.push(
      chartBar(writeX, baseline - writeHeight, barWidth, writeHeight, '#2563eb', 'Write', result, label),
      chartBar(readX, baseline - readHeight, barWidth, readHeight, '#f97316', 'Read', result, label),
      `<text x="${center}" y="${baseline + 20}" text-anchor="end" transform="rotate(-38 ${center} ${
        baseline + 20
      })" class="label">${escapeXml(label)}</text>`
    );
  });

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="title description">
  <title id="title">Storage benchmark write and read throughput</title>
  <desc id="description">Grouped bar chart comparing logical source write throughput and sync stream read throughput in mebibytes per second.</desc>
  <style>
    text { font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #172033; }
    .title { font-size: 22px; font-weight: 700; }
    .subtitle { font-size: 13px; fill: #526079; }
    .tick { font-size: 12px; fill: #526079; }
    .label { font-size: 11px; fill: #344054; }
    .legend { font-size: 13px; font-weight: 600; }
  </style>
  <rect width="100%" height="100%" fill="#ffffff"/>
  <text x="${margin.left}" y="35" class="title">Storage benchmark throughput</text>
  <text x="${margin.left}" y="58" class="subtitle">Logical source writes vs. sync stream reads (MiB/s)</text>
  <rect x="${width - 235}" y="26" width="14" height="14" rx="2" fill="#2563eb"/>
  <text x="${width - 213}" y="38" class="legend">Write</text>
  <rect x="${width - 145}" y="26" width="14" height="14" rx="2" fill="#f97316"/>
  <text x="${width - 123}" y="38" class="legend">Read</text>
  <text x="24" y="${margin.top + plotHeight / 2}" text-anchor="middle" transform="rotate(-90 24 ${
    margin.top + plotHeight / 2
  })" class="legend">MiB/s</text>
  ${elements.join('\n  ')}
  <line x1="${margin.left}" y1="${baseline}" x2="${width - margin.right}" y2="${baseline}" stroke="#667085" stroke-width="1.5"/>
</svg>
`;

  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, svg, 'utf8');
}

function chartBar(
  x: number,
  y: number,
  width: number,
  height: number,
  color: string,
  metric: string,
  result: StorageBenchmarkResult,
  label: string
) {
  const value = metric == 'Write' ? result.write_mebibytes_per_second : result.read_mebibytes_per_second;
  return `<rect x="${x}" y="${y}" width="${width}" height="${height}" rx="2" fill="${color}"><title>${escapeXml(
    `${label}: ${metric} ${formatNumber(value)} MiB/s`
  )}</title></rect>`;
}

function niceCeiling(value: number) {
  if (!Number.isFinite(value) || value <= 0) {
    return 1;
  }
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const normalized = value / magnitude;
  const nice = normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return nice * magnitude;
}

function escapeXml(value: string) {
  return value.replace(/[&<>"']/g, (character) => {
    return {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&apos;'
    }[character]!;
  });
}

function printHelp() {
  console.log(`Compare sync bucket storage benchmarks.

Usage:
  pnpm benchmark:storage [options]

Options:
  -s, --storage <ids>  Comma-separated storage IDs; repeatable. Use "all" for every implementation.
  -o, --output <path>  Write the combined JSON results to this path.
  -c, --chart <path>   Write the SVG chart to this path (default: storage-benchmark-comparison.svg).
  -h, --help           Show this help.

Storage IDs:
${STORAGE_RUNNERS.map((runner) => `  ${runner.id.padEnd(18)} ${runner.label}`).join('\n')}

Database-backed runners use their module's normal test connection environment variables.`);
}

if (process.argv[1] != null && import.meta.url == pathToFileURL(resolve(process.argv[1])).href) {
  try {
    await main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
