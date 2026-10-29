import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import { writeFile } from 'node:fs/promises';
import { setTimeout } from 'node:timers/promises';
import { expect, test, vi } from 'vitest';
import { OperationBatch } from '../../src/storage/implementation/OperationBatch.js';
import { PersistedBatchV3 } from '../../src/storage/implementation/v3/PersistedBatchV3.js';
import { SourceRecordStoreV3 } from '../../src/storage/implementation/v3/SourceRecordStoreV3.js';
import { env } from './env.js';
import { PublicationCommandMonitor } from './helpers/PublicationCommandMonitor.js';

/**
 * Opt-in scheduling experiment with real MongoDB and injected I/O latency.
 * Run with POWERSYNC_PIPELINE_BENCHMARK=1 and --silent=false through the MongoDB test wrapper.
 * Set POWERSYNC_PIPELINE_BENCHMARK_OUTPUT to also save the JSON report.
 * Serial mode drains at every input boundary; both modes keep identical batch
 * targets, payloads and command instrumentation. Timing ends at durable commit.
 * This isolates overlap, rather than predicting production throughput or RSS.
 */
test.skipIf(!process.env.POWERSYNC_PIPELINE_BENCHMARK)(
  'measures serial versus pipelined v4 publication',
  async () => {
    const batchRows = 100;
    const totalRows = 2000;
    const readDelayMs = 10;
    const publicationDelayMs = 20;
    using _batch = vi.spyOn(OperationBatch.prototype, 'shouldFlush').mockImplementation(function (
      this: OperationBatch
    ) {
      return this.length >= batchRows;
    });
    const results: unknown[] = [];
    for (const serial of [true, false]) {
      const gen = mongoTestStorageFactoryGenerator({ url: env.MONGO_TEST_URL, isCI: env.CI, monitorCommands: true });
      await using factory = await gen.factory();
      const stream = await factory.updateSyncRules(
        updateSyncRulesFromYaml(
          `
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM items
`,
          { storageVersion: 4 }
        )
      );
      const buckets = await getTestStorage(factory, stream);
      await using writer = await buckets.createWriter(test_utils.BATCH_OPTIONS);
      const table = await test_utils.resolveTestTable(writer, 'items', ['id'], gen, 1);
      await writer.markAllSnapshotDone('1/1');
      await writer.save({
        sourceTable: table,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'warmup', description: 'warmup' },
        afterReplicaId: 'warmup'
      });
      await writer.commit('1/1');
      const load = SourceRecordStoreV3.prototype.loadDocuments;
      using _read = vi.spyOn(SourceRecordStoreV3.prototype, 'loadDocuments').mockImplementation(async function (
        this: SourceRecordStoreV3,
        ...args
      ) {
        await setTimeout(readDelayMs);
        return load.apply(this, args);
      });
      const publish = PersistedBatchV3.prototype.publish;
      using _publish = vi.spyOn(PersistedBatchV3.prototype, 'publish').mockImplementation(async function (
        this: PersistedBatchV3,
        ...args
      ) {
        await setTimeout(publicationDelayMs);
        return publish.apply(this, args);
      });
      using monitor = new PublicationCommandMonitor(factory.db.client);
      const peak = process.memoryUsage();
      const startedAt = performance.now();
      const description = 'x'.repeat(128);
      for (let i = 0; i < totalRows; i++) {
        await writer.save({
          sourceTable: table,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: String(i), description },
          afterReplicaId: String(i)
        });
        if ((i + 1) % batchRows === 0) {
          if (serial) {
            await writer.flush();
          }
          const memory = process.memoryUsage();
          for (const key of ['rss', 'heapUsed', 'external', 'arrayBuffers'] as const) {
            peak[key] = Math.max(peak[key], memory[key]);
          }
        }
      }
      await writer.commit('1/2');
      const elapsedMs = performance.now() - startedAt;
      const summary = monitor.summary();
      expect((await buckets.getCheckpoint()).lsn).toBe('1/2');
      results.push({
        serial,
        totalRows,
        batchRows,
        readDelayMs,
        publicationDelayMs,
        elapsedMs,
        rowsPerSecond: (totalRows * 1000) / elapsedMs,
        sampledProcessMemoryPeak: peak,
        ...summary
      });
    }
    const report = { benchmark: 'v4-publication-pipeline', node: process.version, results };
    console.info(JSON.stringify(report));
    if (process.env.POWERSYNC_PIPELINE_BENCHMARK_OUTPUT) {
      await writeFile(process.env.POWERSYNC_PIPELINE_BENCHMARK_OUTPUT, JSON.stringify(report, null, 2));
    }
  },
  30_000
);
