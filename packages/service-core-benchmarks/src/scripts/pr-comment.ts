import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { type BenchmarkReport, renderPullRequestComment } from '../utils/benchmark-report.js';
import { artifactRoot, atomicWrite, readSuite } from '../utils/output.js';

const { values } = parseArgs({ options: { 'run-id': { type: 'string' } } });
const { directory } = await readSuite(artifactRoot, values['run-id']);

const report: BenchmarkReport = JSON.parse(await readFile(join(directory, 'report', 'output.json'), 'utf8'));

const workflowUrl = process.env.BENCHMARK_WORKFLOW_URL;
const artifactUrl = process.env.BENCHMARK_ARTIFACT_URL;

if (!workflowUrl) throw new Error('BENCHMARK_WORKFLOW_URL is required');

const body = renderPullRequestComment(report, { workflowUrl, artifactUrl });
await atomicWrite(join(directory, 'report', 'comment.md'), body);
