import { appendFile } from 'node:fs/promises';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { createReport, loadResults, printReport, renderMarkdown } from '../utils/benchmark-report.js';
import { artifactRoot, atomicWrite, readSuite } from '../utils/output.js';

const { values } = parseArgs({ options: { 'run-id': { type: 'string' } } });
const { directory, manifest } = await readSuite(artifactRoot, values['run-id']);
const report = createReport(manifest, await loadResults(directory));
const markdown = renderMarkdown(report);
await atomicWrite(join(directory, 'report', 'output.json'), `${JSON.stringify(report, null, 2)}\n`);
await atomicWrite(join(directory, 'report', 'output.md'), markdown);
if (process.env.GITHUB_STEP_SUMMARY) await appendFile(process.env.GITHUB_STEP_SUMMARY, markdown);
printReport(report);
console.info(`Report saved to ${join(directory, 'report')}`);
