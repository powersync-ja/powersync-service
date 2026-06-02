import { afterAll, describe, test } from 'vitest';
import { env } from '../env.js';
import { ConvexStreamTestContext } from '../test-utils/ConvexStreamTestContext.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from '../test-utils/util.js';

/**
 * Vitest has built in benchmarking functionality, but, it seems non-trivial to preseed the database.
 * Run this by running:
 * ```bash
 * export SHOULD_RUN_BENCHMARK=true
 * vitest initial_replication.test.ts
 * ```
 */
const SEED_BATCH_SIZE = 1_000;
const ESTIMATED_STORAGE_BYTES_PER_1K_ROWS = 350 * 1024;

const BENCHMARK_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT uuid as id, name FROM "lists"
      - SELECT uuid as id, description, list_uuid FROM "todos"
`;

describe
  .skipIf(!env.SHOULD_RUN_BENCHMARK)
  .sequential('Convex initial replication benchmark', { timeout: Infinity }, () => {
    let testResults = new Map<number, number>();

    afterAll(() => {
      console.log('Test results');
      console.log('| Total Rows | Estimated Size (MiB) | Elapsed Time (ms) | Estimated Rate (MiB/s) |');
      for (const [totalRows, elapsedTime] of testResults.entries()) {
        const estimatedSize = estimateStorageSize(totalRows);
        const estimatedRate = estimateReplicationRate(estimatedSize.bytes, elapsedTime);
        console.log(
          `| ${totalRows} | ${estimatedSize.mib.toFixed(2)} | ${Math.round(elapsedTime)} | ${estimatedRate.toFixed(
            2
          )} |`
        );
      }
    });

    function defineTest({ totalRows }: { totalRows: number }) {
      return test(`starting initial replication for ${totalRows} total rows`, async () => {
        await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
          doNotClear: false // Will clear the source (Convex) database
        });
        // seed with data
        await seedBenchmarkRows(context, totalRows);

        await context.updateSyncRules(BENCHMARK_SYNC_RULES);

        const startedAt = performance.now();
        await context.replicateSnapshot();
        const elapsedMs = performance.now() - startedAt;

        console.info(`Initial Convex replication benchmark replicated ${totalRows} rows in ${Math.round(elapsedMs)}ms`);

        testResults.set(totalRows, elapsedMs);
      });
    }

    // Now register the tests for each dataset
    // Storage estimate is based on current observed storage use: 1_000 rows ~= 350 KiB.
    defineTest({
      totalRows: 10_000
    });

    defineTest({
      totalRows: 50_000
    });

    defineTest({
      totalRows: 100_000
    });
  });

function estimateStorageSize(totalRows: number) {
  const bytes = (totalRows / 1_000) * ESTIMATED_STORAGE_BYTES_PER_1K_ROWS;
  return {
    bytes,
    mib: bytes / 1024 / 1024
  };
}

function estimateReplicationRate(bytes: number, elapsedMs: number) {
  return bytes / 1024 / 1024 / (elapsedMs / 1_000);
}

async function seedBenchmarkRows(context: ConvexStreamTestContext, totalRows: number) {
  const totalRowsPerTable = Math.floor(totalRows / 2);
  for (let counter = 0; counter < totalRowsPerTable; counter += SEED_BATCH_SIZE) {
    const count = Math.min(SEED_BATCH_SIZE, totalRowsPerTable - counter);
    await context.backend.client.mutation(context.backend.api.benchmark.seedInitialReplicationBatch, {
      rowsPerTable: count
    });

    console.info(`Seeded ${(counter + count) * 2}/${totalRows} rows.`);
    /**
     * Nasty workarround for default Convex write limits
     * {"code":"TooManyWrites","message":"Too many writes per second. Your deployment is limited to 4 MiB bytes written per 1 second. Reduce your write rate or upgrade to a larger deployment."}
     */
    await new Promise((r) => setTimeout(r, 1_000));
  }
}
