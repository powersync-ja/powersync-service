import { mongo } from '@powersync/lib-service-mongodb';
import { SourceTable } from '@powersync/service-core';
import { HydratedSyncConfig } from '@powersync/service-sync-rules';
import { MongoManager } from './MongoManager.js';
import type { MongoReplicationStreamBatch, MongoReplicationStreamContext } from './MongoReplicationStreamTypes.js';

/**
 * Sync config and source namespace information used to construct a provider for a replication attempt.
 */
export interface MongoReplicationQueryProviderContext {
  /**
   * Hydrated sync config from which the provider can read its connection-specific options.
   */
  syncConfig: HydratedSyncConfig;
  /**
   * Identifies the source connection whose options and tables this provider handles.
   */
  connectionTag: string;
  /**
   * MongoDB database used to resolve table references without an explicit schema.
   */
  defaultSchema: string;
}

/**
 * Adapts snapshot selection and the stream of source changes for one replication implementation.
 * The MongoDB module owns cursor options, namespaces, BSON parsing, large-event reassembly and durable progress.
 */
export interface MongoReplicationQueryProvider {
  /**
   * Validate source capabilities before either snapshot queries or streaming cursors are opened.
   * Throw if required capabilities are unavailable. Both the stream and snapshotter may call this hook
   * on the same provider, so validation must be safe to repeat.
   */
  validateSource?(options: { connectionManager: MongoManager; isDocumentDb: boolean }): Promise<void>;

  /**
   * Call context.open() with any additional stages/pre-image options. For filtering, pass its result
   * to filterMongoReplicationStream({ batches, filter }) with a MongoReplicationFilter callback.
   * The helper preserves checkpoints, DDL, batch boundaries, buffered-change flags and safe progress,
   * counts excluded events, and closes the source iterator on failure or early exit.
   * The default provider returns the opener's batches directly.
   * Each invocation is independent and also serves snapshot barriers and resume validation; keep
   * filter state local to that invocation. Snapshot selection must agree with streaming decisions.
   */
  openChangeStream(context: MongoReplicationStreamContext): AsyncIterableIterator<MongoReplicationStreamBatch>;

  /**
   * Returns an additional MongoDB find predicate for a physical source table.
   * Return null to snapshot without an additional filter. Snapshot selection must agree with the
   * provider's streaming rules so both paths replicate the same set of documents.
   * Expressions can be supplied as { $expr: ... }. The snapshot query combines the filter with its
   * _id continuation predicate and uses simple collation whenever a filter is supplied.
   */
  getSnapshotFilter(table: SourceTable): mongo.Filter<mongo.Document> | null;
}

/**
 * Creates a provider shared by a replication attempt's change stream, snapshotter and source probes.
 */
export type MongoReplicationQueryProviderFactory = (
  context: MongoReplicationQueryProviderContext
) => MongoReplicationQueryProvider;

/**
 * Uses the standard MongoDB reader and snapshot selection without additional filtering or event translation.
 */
export const DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER: MongoReplicationQueryProvider = {
  getSnapshotFilter: () => null,
  openChangeStream: ({ open }) => open({})
};

/**
 * Places provider stages after namespace selection and before optional large-event splitting.
 * This lets provider stages inspect complete events; the shared reader reassembles any resulting fragments.
 */
export function buildMongoChangeStreamPipeline(options: {
  /**
   * Shared reader options, including resume position, stream scope and requested document images.
   */
  streamOptions: mongo.ChangeStreamOptions & mongo.Document;
  /**
   * Namespace predicate restricting the stream to the selected source collections and checkpoint events.
   */
  namespaceMatch: mongo.Document;
  /**
   * Additional aggregation stages supplied by the provider, in execution order.
   */
  pipelineStages: mongo.Document[];
  /**
   * Append MongoDB's large-event split stage. Disabled for DocumentDB by the shared reader.
   */
  includeSplitLargeEvent: boolean;
}): mongo.Document[] {
  const pipeline: mongo.Document[] = [
    { $changeStream: options.streamOptions },
    { $match: options.namespaceMatch },
    ...options.pipelineStages
  ];

  if (options.includeSplitLargeEvent) {
    pipeline.push({ $changeStreamSplitLargeEvent: {} });
  }

  return pipeline;
}
