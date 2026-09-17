import { mongo } from '@powersync/lib-service-mongodb';
import { SourceTable } from '@powersync/service-core';
import { HydratedSyncConfig } from '@powersync/service-sync-rules';
import { MongoManager } from './MongoManager.js';
import { MongoReplicationStreamContext, MongoReplicationStreamItem } from './MongoReplicationStream.js';

export interface MongoReplicationQueryProviderContext {
  syncConfig: HydratedSyncConfig;
  connectionTag: string;
  defaultSchema: string;
}

/**
 * Adapts snapshot selection and the stream of source changes for one replication implementation.
 * The MongoDB module owns cursor options, namespaces, BSON parsing, large-event reassembly and durable progress.
 */
export interface MongoReplicationQueryProvider {
  /** Validate source capabilities before either snapshot queries or streaming cursors are opened. */
  validateSource?(options: { connectionManager: MongoManager; isDocumentDb: boolean }): Promise<void>;

  /**
   * Call context.open() with any additional stages/pre-image options, then optionally wrap its iterator.
   * Unlike pipeline stages alone, the wrapper can exclude marked events while still forwarding progress.
   * Yield changes and safe progress, including when all data events are excluded.
   * An exit from a filtered set may be represented as a synthetic delete retaining its original key and token.
   * Checkpoint/DDL events must remain ordered and intact. Progress alone must never stand in for a checkpoint.
   */
  openChangeStream(context: MongoReplicationStreamContext): AsyncIterableIterator<MongoReplicationStreamItem>;

  /**
   * Returns an additional MongoDB find predicate for a physical source table.
   * Expressions can be supplied as { $expr: ... }. The snapshot query combines the filter with its
   * _id continuation predicate and uses simple collation whenever a filter is supplied.
   */
  getSnapshotFilter(table: SourceTable): mongo.Filter<mongo.Document> | null;
}

export type MongoReplicationQueryProviderFactory = (
  context: MongoReplicationQueryProviderContext
) => MongoReplicationQueryProvider;

export const DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER: MongoReplicationQueryProvider = {
  getSnapshotFilter: () => null,
  openChangeStream: ({ open }) => open({})
};

export function buildMongoChangeStreamPipeline(options: {
  streamOptions: mongo.ChangeStreamOptions & mongo.Document;
  namespaceMatch: mongo.Document;
  pipelineStages: mongo.Document[];
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
