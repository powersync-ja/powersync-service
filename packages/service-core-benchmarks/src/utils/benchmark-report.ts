import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { BenchmarkResult, BenchmarkStatisticSummary } from '../types/BenchmarkResult.js';
import type { StorageBenchmarkScenario } from '../types/StorageBenchmark.js';
import { summarizeBenchmarkIterations } from './benchmark-summary.js';
import type { SuiteManifest } from './output.js';

export interface BenchmarkReport {
  schema_version: 1;
  run: SuiteManifest;
  results: BenchmarkResult[];
}
export type ReportRow = Record<string, string | number>;

const percentileNote =
  'Statistics use successful measured iterations only. p95 requires 20 samples; p99 requires 100. Sample counts are shown for each metric.';
const resourceNote =
  'CPU and memory cover the monitor execution window, not individual timing boundaries. RSS is process memory; database memory is Docker container usage excluding inactive file cache. Peaks are sampled observations; child monitors sample only the start and end.';

export function createReport(run: SuiteManifest, results: readonly BenchmarkResult[]): BenchmarkReport {
  return {
    schema_version: 1,
    run,
    results: [...results]
      .sort((a, b) => a.scenario.id.localeCompare(b.scenario.id))
      .map((result) => ({
        ...result,
        summary: result.iterations.length
          ? summarizeBenchmarkIterations(result.iterations, result.scenario.layer)
          : null
      }))
  };
}

export async function loadResults(directory: string): Promise<BenchmarkResult[]> {
  const names = (await readdir(join(directory, 'json'))).filter((name) => name.endsWith('.json')).sort();
  return Promise.all(
    names.map(async (name) => {
      try {
        const result: unknown = JSON.parse(await readFile(join(directory, 'json', name), 'utf8'));
        validateResult(result);
        // Also associate semantic/metric validation errors with their artifact filename.
        summarizeBenchmarkIterations(result.iterations, result.scenario.layer);
        return result;
      } catch (error) {
        throw new Error(
          `Invalid benchmark artifact ${name}: ${error instanceof Error ? error.message : String(error)}`,
          { cause: error }
        );
      }
    })
  );
}

function record(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}

function validateResult(value: unknown): asserts value is BenchmarkResult {
  if (
    !record(value) ||
    !record(value.scenario) ||
    typeof value.scenario.id !== 'string' ||
    !['storage', 'replication', 'api', 'combined'].includes(String(value.scenario.layer)) ||
    !['passed', 'failed', 'skipped'].includes(String(value.status)) ||
    !Array.isArray(value.iterations) ||
    !Array.isArray(value.errors) ||
    !record(value.environment)
  ) {
    throw new Error('Invalid result structure');
  }

  for (const iteration of value.iterations) {
    if (
      !record(iteration) ||
      !['warmup', 'measured'].includes(String(iteration.kind)) ||
      !['passed', 'failed'].includes(String(iteration.status)) ||
      !record(iteration.boundaries) ||
      !record(iteration.counters) ||
      !Array.isArray(iteration.resources) ||
      !Array.isArray(iteration.errors)
    ) {
      throw new Error('Invalid iteration structure');
    }

    for (const boundary of Object.values(iteration.boundaries)) {
      if (
        !record(boundary) ||
        typeof boundary.duration_ms !== 'number' ||
        !Number.isFinite(boundary.duration_ms) ||
        boundary.duration_ms < 0
      ) {
        throw new Error('Invalid timing boundary');
      }
    }

    for (const counter of Object.values(iteration.counters)) {
      if (typeof counter !== 'number' || !Number.isFinite(counter) || counter < 0) {
        throw new Error('Invalid counter');
      }
    }

    for (const resource of iteration.resources) {
      if (
        !record(resource) ||
        typeof resource.component !== 'string' ||
        !['available', 'unavailable', 'failed'].includes(String(resource.status)) ||
        !record(resource.metrics) ||
        !Array.isArray(resource.errors) ||
        !(resource.reason == null || typeof resource.reason === 'string')
      ) {
        throw new Error('Invalid resource monitor result');
      }
    }
  }
}

function primaryBoundary(result: BenchmarkResult): string {
  switch (result.scenario.layer) {
    case 'storage':
      return (result.scenario as StorageBenchmarkScenario).primary_boundary;
    case 'api':
      return 'http_read';
    case 'combined':
      return 'end_to_end_snapshot';
    case 'replication':
      return result.summary?.boundaries.replication_streaming ? 'replication_streaming' : 'replication_snapshot';
  }
}

function format(value: number | undefined): string {
  if (value == null) return '—';
  if (value !== 0 && Math.abs(value) < 0.001) return value.toExponential(3);
  return Number(value.toPrecision(6)).toLocaleString('en-US', { maximumFractionDigits: 6 });
}

export function overviewRows(report: BenchmarkReport): ReportRow[] {
  return report.results.map((result) => {
    const boundary = primaryBoundary(result);
    const rateCounter = result.scenario.layer === 'combined' ? 'client_operations' : 'bucket_operations';
    const timing = result.summary?.boundaries[boundary];
    const rate = result.summary?.rates[`${boundary}.${rateCounter}_per_second`]?.statistics;
    return {
      Scenario: result.scenario.id,
      Status: result.status,
      'Successful iterations': result.summary?.successful_iterations ?? 0,
      'Failed iterations': result.summary?.failed_iterations ?? 0,
      Boundary: boundary,
      'Min (ms)': format(timing?.min),
      'Average (ms)': format(timing?.avg),
      'Max (ms)': format(timing?.max),
      'Min rate (ops/s)': format(rate?.min),
      'Average rate (ops/s)': format(rate?.avg),
      'Max rate (ops/s)': format(rate?.max)
    };
  });
}

export function metricRows(result: BenchmarkResult): ReportRow[] {
  if (!result.summary) return [];

  const summary = result.summary;
  const metrics: {
    group: string;
    name: string;
    unit: string;
    statistics: BenchmarkStatisticSummary | null;
    note?: string;
  }[] = [];

  for (const [name, statistics] of Object.entries(summary.boundaries)) {
    metrics.push({ group: 'Timing', name, unit: statistics.unit, statistics });
  }

  for (const [name, statistics] of Object.entries(summary.counters)) {
    metrics.push({ group: 'Counter', name, unit: statistics.unit, statistics });
  }

  for (const [name, rate] of Object.entries(summary.rates)) {
    metrics.push({
      group: 'Rate',
      name,
      unit: rate.unit,
      statistics: rate.statistics,
      note: `${rate.counter} / ${rate.boundary}${rate.reason ? `; ${rate.excluded_samples} excluded: ${rate.reason}` : ''}`
    });
  }

  for (const [component, resource] of Object.entries(summary.resources)) {
    for (const [name, statistics] of Object.entries(resource.metrics))
      metrics.push({ group: 'Resource', name: `${component}.${name}`, unit: statistics.unit, statistics });
  }

  const showP95 = metrics.some((metric) => metric.statistics?.p95 != null);
  const showP99 = metrics.some((metric) => metric.statistics?.p99 != null);
  return metrics.map(({ group, name, unit, statistics, note }) => ({
    Group: group,
    Metric: name,
    Unit: unit,
    Samples: statistics?.sample_count ?? 0,
    Min: format(statistics?.min),
    Median: format(statistics?.median),
    Average: format(statistics?.avg),
    ...(showP95 ? { p95: format(statistics?.p95) } : {}),
    ...(showP99 ? { p99: format(statistics?.p99) } : {}),
    Max: format(statistics?.max),
    Notes: note ?? ''
  }));
}

export function escapeMarkdown(value: unknown): string {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replace(/[\\`*_{}\[\]()#!|~]/g, '\\$&')
    .replace(/[\r\n]+/g, ' ');
}

function table(rows: ReportRow[]): string {
  if (!rows.length) return '';

  const headers = Object.keys(rows[0]);
  return [headers, headers.map(() => '---'), ...rows.map((row) => headers.map((header) => row[header]))]
    .map((cells) => `| ${cells.map(escapeMarkdown).join(' | ')} |`)
    .join('\n');
}

function header(report: BenchmarkReport): string {
  const { run } = report;
  return (
    `# Benchmark results\n\nRun: ${escapeMarkdown(run.run_id)} · Status: **${escapeMarkdown(run.status)}**\n\n` +
    `Commit: ${escapeMarkdown(run.git_sha ?? 'unknown')}\n\nStarted: ${escapeMarkdown(run.started_at)} · Finished: ${escapeMarkdown(run.finished_at ?? 'not recorded')}\n\n` +
    `Arguments: ${escapeMarkdown(run.args.join(' ') || '(none)')} · Environment filter: ${escapeMarkdown(run.tags_filter ?? '(none)')}\n\n` +
    (report.results.length ? '' : 'No benchmark results were recorded for this run.\n\n')
  );
}

function diagnostics(result: BenchmarkResult): string[] {
  const notes = result.errors.map((error) => `${error.phase}: ${error.message}`);
  for (const iteration of result.iterations) {
    for (const error of iteration.errors) {
      notes.push(`${iteration.kind} iteration ${iteration.iteration}, ${error.phase}: ${error.message}`);
    }

    for (const check of iteration.correctness?.checks ?? []) {
      if (!check.passed) {
        notes.push(`${iteration.kind} iteration ${iteration.iteration}: correctness check failed: ${check.name}`);
      }
    }
  }

  for (const [component, resource] of Object.entries(result.summary?.resources ?? {})) {
    notes.push(
      `${component} monitor: ${resource.available} available, ${resource.unavailable} unavailable, ${resource.failed} failed${resource.reasons.length ? ` (${resource.reasons.join('; ')})` : ''}`
    );
  }

  return notes;
}

function details(report: BenchmarkReport): string {
  return report.results
    .map(
      (result) =>
        `## ${escapeMarkdown(result.scenario.id)}\n\n` +
        `Status: ${escapeMarkdown(result.status)}\n\n` +
        (result.summary ? table(metricRows(result)) : 'No measured statistics available.') +
        '\n\n' +
        diagnostics(result)
          .map((note) => `- ${escapeMarkdown(note)}`)
          .join('\n')
    )
    .join('\n\n');
}

export function renderMarkdown(report: BenchmarkReport): string {
  return `${header(report)}${table(overviewRows(report))}\n\n${percentileNote}\n\n${resourceNote}\n\n${details(report)}\n`;
}

export function printReport(report: BenchmarkReport): void {
  console.info(`Benchmark run ${report.run.run_id}: ${report.run.status} (commit ${report.run.git_sha ?? 'unknown'})`);
  if (!report.results.length) console.info('No benchmark results were recorded for this run.');
  console.table(overviewRows(report));
  console.info(percentileNote);
  console.info(resourceNote);
  for (const result of report.results) {
    console.info(`\n${'='.repeat(80)}\n${result.scenario.id}: ${result.status}\n${'='.repeat(80)}\n`);
    console.table(metricRows(result));
    for (const note of diagnostics(result)) console.info(note);
    console.info('');
  }
}

export function renderPullRequestComment(
  report: BenchmarkReport,
  links: { workflowUrl: string; artifactUrl?: string },
  limit = 60_000
): string {
  const footer = `\n\n[Workflow run](${links.workflowUrl})${links.artifactUrl ? ` · [Complete benchmark artifacts](${links.artifactUrl})` : ''}`;
  const prefix = header(report);
  const rows = overviewRows(report);
  const overview = table(rows);
  const expanded = `${prefix}${overview}\n\n${percentileNote}\n\n<details>\n<summary>Detailed metrics</summary>\n\n${resourceNote}\n\n${details(report)}\n\n</details>${footer}`;
  if (expanded.length <= limit) return expanded;

  const notice = '\n\nReport truncated for the PR comment. See the complete report in the workflow artifacts.';
  while (rows.length && (prefix + table(rows) + notice + footer).length > limit) rows.pop();
  const compact = prefix + table(rows) + notice + footer;
  if (compact.length <= limit) return compact;

  // Extremely long selection arguments must not displace links or break Markdown structure.
  return `# Benchmark results\n\nStatus: **${report.run.status}**${notice}${footer}`;
}
