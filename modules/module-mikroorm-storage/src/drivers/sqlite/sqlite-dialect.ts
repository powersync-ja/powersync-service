import type { SqlEntityManager } from '@mikro-orm/sql';
import {
  BucketData,
  BucketParameters,
  CurrentData,
  Instance,
  SourceTable,
  SyncRules,
  WriteCheckpoint
} from '../../entities/entities-index.js';
import { MikroOrmCheckpointWatcher, MikroOrmStorageDialect } from '../../storage/MikroOrmStorageDialect.js';
import { MIKRO_ORM_SQLITE_STORAGE_TYPE } from '../../types/types.js';

export class SqliteCheckpointWatcher implements MikroOrmCheckpointWatcher {
  private readonly listeners = new Set<() => void>();

  notify(): void {
    for (const listener of this.listeners) {
      listener();
    }
  }

  async *watch(signal: AbortSignal): AsyncIterable<void> {
    while (!signal.aborted) {
      yield await new Promise<void>((resolve) => {
        const listener = () => {
          this.listeners.delete(listener);
          resolve();
        };
        this.listeners.add(listener);
        signal.addEventListener(
          'abort',
          () => {
            this.listeners.delete(listener);
            resolve();
          },
          { once: true }
        );
      });
    }
  }
}

export const sqliteMikroOrmStorageDialect: MikroOrmStorageDialect = {
  type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
  entityClasses: [BucketData, BucketParameters, CurrentData, Instance, SourceTable, SyncRules, WriteCheckpoint],
  bucketDataEntity: BucketData,
  bucketParametersEntity: BucketParameters,
  currentDataEntity: CurrentData,
  instanceEntity: Instance,
  sourceTableEntity: SourceTable,
  syncRulesEntity: SyncRules,
  writeCheckpointEntity: WriteCheckpoint,
  async *streamBucketDataRows(options) {
    if (options.dataBuckets.length == 0) {
      return;
    }

    // SQLite reads can complete synchronously enough that tight polling loops
    // starve replication/checkpoint work in the same process. Yield once before
    // the query so single-process unified mode remains cooperative.
    await new Promise<void>((resolve) => setImmediate(resolve));

    const filters = options.dataBuckets.map((request) => ({
      bucketName: request.bucket,
      opId: { $gt: request.start, $lte: options.checkpoint }
    }));
    const sqlEntityManager = options.em as SqlEntityManager;
    const queryBuilder = sqlEntityManager
      .createQueryBuilder(BucketData, 'bucket_data')
      .select('*')
      .where({
        groupId: options.groupId,
        $or: filters
      })
      .orderBy({ bucketName: 'ASC', opId: 'ASC' })
      .limit(options.limit);

    yield* streamQueryBuilder(queryBuilder.stream());
  },
  createCheckpointWatcher: () => new SqliteCheckpointWatcher()
};

async function* streamQueryBuilder<T>(stream: AsyncIterableIterator<T>): AsyncIterable<T> {
  const iterator = stream[Symbol.asyncIterator]();
  try {
    while (true) {
      const result = await iterator.next();
      if (result.done) {
        return;
      }
      yield result.value;
    }
  } finally {
    await iterator.return?.();
  }
}
