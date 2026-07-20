import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  CheckpointChanges,
  GetCheckpointChangesOptions,
  InternalOpId,
  internalToExternalOpId,
  ParameterSetLimitExceededError,
  ProtocolOpId,
  SingleSyncConfigBucketDefinitionMapping,
  storage,
  utils
} from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { ParameterLookupRows, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { idPrefixFilter, mapOpEntry, readSingleBatch, setSessionSnapshotTime } from '../../../utils/util.js';
import { MongoBucketStorage } from '../../MongoBucketStorage.js';
import { BucketDataDoc } from '../common/BucketDataDoc.js';
import { MongoSyncBucketStorageCheckpoint } from '../common/MongoSyncBucketStorageCheckpoint.js';
import { MongoChecksums } from '../MongoChecksums.js';
import { MongoCompactOptions, MongoCompactor } from '../MongoCompactor.js';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { MongoPersistedReplicationStream } from '../MongoPersistedReplicationStream.js';
import {
  BucketRowEstimate,
  MongoSyncBucketStorage,
  MongoSyncBucketStorageOptions,
  TopBucketCandidate,
  TopBucketSelection,
  TopDefinitionCandidate
} from '../MongoSyncBucketStorage.js';
import { loadBucketDataDocument } from './bucket-format.js';
import {
  BucketDataDocumentV3,
  BucketParameterDocumentV3,
  deserializeParameterLookup,
  ReplicationStreamDocumentV3,
  serializeParameterLookup,
  SyncRuleConfigStateV3
} from './models.js';
import { MongoBucketBatchV3 } from './MongoBucketBatchV3.js';
import { MongoChecksumsV3 } from './MongoChecksumsV3.js';
import { MongoCompactorV3 } from './MongoCompactorV3.js';
import { MongoStoppedSyncConfigCleanup } from './MongoStoppedSyncConfigCleanup.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

export interface MongoSyncBucketStorageContextV3 {
  db: VersionedPowerSyncMongoV3;
  replicationStreamId: number;
  readPreference?: mongo.ReadPreference;
  /**
   * Persisted mapping of the single sync config that read operations are served from.
   *
   * Implemented as a lazy getter: accessing it on a storage instance with multiple sync
   * configs throws, but operations that don't use it remain unaffected.
   */
  readonly mapping: SingleSyncConfigBucketDefinitionMapping;
}

function* walkDocumentOps(
  data: BucketDataDoc[],
  documentOpCounts: number[],
  documentSizes: number[]
): Generator<{ row: BucketDataDoc; docIndex: number; isLastOpInDocument: boolean }> {
  let opIndex = 0;
  for (const [docIndex, opCount] of documentOpCounts.entries()) {
    for (let i = 0; i < opCount; i++) {
      yield { row: data[opIndex++], docIndex, isLastOpInDocument: i === opCount - 1 };
    }
  }
}

function extractRowsFromDocument(
  doc: BucketDataDocumentV3,
  context: { replicationStreamId: number; definitionId: string },
  bucketMap: Map<string, InternalOpId>,
  endOpId: InternalOpId,
  remainingLimit: number
): { rows: BucketDataDoc[]; remainingLimit: number; limitReached: boolean } {
  const rows: BucketDataDoc[] = [];
  for (const row of loadBucketDataDocument(context, doc)) {
    const bucket = row.bucketKey.bucket;
    const bucketStart = bucketMap.get(bucket);
    if (bucketStart == null) {
      throw new Error(`data for unexpected bucket: ${bucket}`);
    }
    if (row.o <= bucketStart) {
      continue;
    }
    if (row.o > endOpId) {
      continue;
    }

    rows.push(row);
    remainingLimit--;
    if (remainingLimit <= 0) {
      return { rows, remainingLimit, limitReached: true };
    }
  }
  return { rows, remainingLimit, limitReached: false };
}

export class MongoSyncBucketStorageV3 extends MongoSyncBucketStorage {
  declare readonly db: VersionedPowerSyncMongoV3;
  declare readonly checksums: MongoChecksumsV3;

  constructor(
    factory: MongoBucketStorage,
    replicationStreamId: number,
    replicationStream: MongoPersistedReplicationStream,
    replicationStreamName: string,
    writeCheckpointMode: storage.WriteCheckpointMode | undefined,
    options: MongoSyncBucketStorageOptions
  ) {
    super(factory, replicationStreamId, replicationStream, replicationStreamName, writeCheckpointMode, options);
    if (replicationStream.syncConfigIds.length == 0) {
      throw new ServiceAssertionError('Missing sync config id for storage v3');
    }
  }

  private get syncConfigIds(): bson.ObjectId[] {
    return this.replicationStream.syncConfigIds;
  }

  private get syncRulesCollection(): mongo.Collection<ReplicationStreamDocumentV3> {
    return this.db.sync_rules as unknown as mongo.Collection<ReplicationStreamDocumentV3>;
  }

  private syncConfigMatch(extra: mongo.Document = {}): mongo.Filter<ReplicationStreamDocumentV3> {
    return {
      _id: this.replicationStreamId,
      sync_configs: {
        $elemMatch: {
          _id: { $in: this.syncConfigIds },
          ...extra
        }
      }
    };
  }

  private syncConfigProjection(extra: mongo.Document = {}): mongo.Document {
    return {
      ...extra,
      sync_configs: 1
    };
  }

  private selectedSyncConfigs(doc: Pick<ReplicationStreamDocumentV3, 'sync_configs'> | null): SyncRuleConfigStateV3[] {
    return doc?.sync_configs?.filter((config) => this.syncConfigIds.some((id) => id.equals(config._id))) ?? [];
  }

  protected async initializeVersionStorage(): Promise<void> {
    const storageIds = this.storageIds;
    for (const source of storageIds.bucketDefinitionIds) {
      const collection = this.db.bucketData(this.replicationStreamId, source).collectionName;
      await this.db.db.createCollection(collection, {}).catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceExists') {
          return;
        }
        throw error;
      });
    }
    for (const indexId of storageIds.parameterIndexIds) {
      await this.db.parameterIndex(this.replicationStreamId, indexId).createIndex(
        {
          lookup: 1,
          key: 1,
          _id: -1
        },
        {
          name: 'lookup_op_id'
        }
      );
    }
  }

  protected createMongoChecksums(options: MongoSyncBucketStorageOptions): MongoChecksums {
    return new MongoChecksumsV3(this.db, this.replicationStreamId, {
      ...options.checksumOptions,
      checksumCacheTtlMs: options.checksumCacheTtlMs,
      storageConfig: options?.storageConfig,
      syncConfigMapping: () => this.singleSyncConfigMapping()
    });
  }

  createMongoCompactor(options: MongoCompactOptions): MongoCompactor {
    return new MongoCompactorV3(this, this.db, options);
  }

  // For storage v3, bucket state is a per-stream collection and bucket data is split into per-definition collections.
  // A replication stream can host multiple sync configs (active + processing + stopped, until cleanup runs), all
  // sharing these collections. Scope to the active config's definition ids so the report excludes stale buckets
  // from old/stopped definitions. `this.storageIds` is derived from the active config only (see getActiveSyncConfig).
  protected async collectTopBuckets(limit: number): Promise<TopBucketSelection> {
    const { buckets, definitions, definitionsTruncated, totals } = await this.aggregateTopBuckets(
      this.db.bucketState(this.replicationStreamId),
      { '_id.d': { $in: this.storageIds.bucketDefinitionIds } },
      limit
    );
    return {
      buckets: buckets.map((b) => ({
        bucket: b.id.b,
        operations: b.operations,
        operationBytes: b.operationBytes,
        defId: b.id.d
      })),
      definitions,
      definitionsTruncated,
      totals
    };
  }

  protected estimateBucketRows(candidate: TopBucketCandidate): Promise<BucketRowEstimate> {
    // v3 batches operations into documents (one doc holds an `ops` array), in a per-definition collection.
    // Sample whole batch documents, then unwind to operation level so the shared estimator sees one doc per op.
    const sampled = this.shouldSampleBucketRows(candidate.operations);
    const collection = this.db.bucketData(this.replicationStreamId, candidate.defId!);
    const buildPrefix = (applySample: boolean): mongo.Document[] => {
      // Range-match on the whole `_id` (b, o) so the {_id} index is used; a dotted `{'_id.b': ...}` match
      // cannot use the compound-object index and would scan the whole collection per bucket.
      const prefix: mongo.Document[] = [
        { $match: { _id: idPrefixFilter<{ b: string; o: unknown }>({ b: candidate.bucket }, ['o']) } }
      ];
      if (applySample) {
        prefix.push({ $match: { $sampleRate: this.bucketRowSampleRate(candidate.operations) } });
      }
      prefix.push({ $unwind: '$ops' }, { $replaceRoot: { newRoot: '$ops' } });
      return prefix;
    };
    return this.estimateRowsFromOperationSample(collection, buildPrefix, candidate.operations, sampled);
  }

  protected estimateDefinitionRows(candidate: TopDefinitionCandidate): Promise<BucketRowEstimate> {
    // A definition's operations are exactly its per-definition bucket_data collection, so no match stage is
    // needed. Keep the bucket name alongside each unwound operation: at definition grain a row counts once
    // per bucket holding it.
    const sampled = this.shouldSampleBucketRows(candidate.operations);
    const collection = this.db.bucketData(this.replicationStreamId, candidate.defId!);
    const buildPrefix = (applySample: boolean): mongo.Document[] => {
      const prefix: mongo.Document[] = [];
      if (applySample) {
        prefix.push({ $match: { $sampleRate: this.bucketRowSampleRate(candidate.operations) } });
      }
      prefix.push(
        { $unwind: '$ops' },
        { $project: { b: '$_id.b', op: '$ops.op', table: '$ops.table', row_id: '$ops.row_id' } }
      );
      return prefix;
    };
    return this.estimateRowsFromOperationSample(collection, buildPrefix, candidate.operations, sampled, {
      b: '$b',
      table: '$table',
      row_id: '$row_id'
    });
  }

  protected createMongoParameterCompactor(
    checkpoint: InternalOpId,
    options: storage.CompactOptions
  ): MongoParameterCompactor {
    return new MongoParameterCompactor(this.db, this.replicationStreamId, checkpoint, options, () =>
      this.db
        .listParameterIndexCollections(this.replicationStreamId)
        .then((collections) =>
          collections.map((c) => c.collection as unknown as lib_mongo.mongo.Collection<lib_mongo.mongo.Document>)
        )
    );
  }

  protected async fetchPersistedOpHead(): Promise<InternalOpId | null> {
    const doc = await this.syncRulesCollection.findOne(
      { _id: this.replicationStreamId },
      { projection: { last_persisted_op: 1 } }
    );
    return doc?.last_persisted_op == null ? null : BigInt(doc.last_persisted_op);
  }

  protected async createWriterImpl(options: storage.CreateWriterOptions): Promise<storage.BucketStorageBatch> {
    const doc = await this.syncRulesCollection.findOne(
      { _id: this.replicationStreamId },
      { projection: { resume_lsn: 1 } }
    );

    return new MongoBucketBatchV3({
      ...this.writerBatchOptions(options),
      // The stream-level replication position - per-config checkpoint LSNs are consistency
      // markers and do not affect where replication resumes.
      resumeFromLsn: doc?.resume_lsn ?? null,
      syncConfigIds: this.syncConfigIds
    });
  }

  protected async fetchCheckpointState(
    session: mongo.ClientSession
  ): Promise<{ checkpoint: bigint; lsn: string | null } | null> {
    const doc = await this.syncRulesCollection.findOne(
      this.syncConfigMatch({
        state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
      }),
      {
        session,
        projection: this.syncConfigProjection()
      }
    );
    // Checkpoints are served from the single active config. A PROCESSING config in the same
    // stream (incremental reprocessing) does not affect checkpoints until it is activated.
    const syncConfigs = this.selectedSyncConfigs(doc).filter((config) =>
      [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED].includes(config.state)
    );
    if (syncConfigs.length > 1) {
      // Activation atomically replaces the previous active config, so this cannot happen unless
      // the stored state is corrupt.
      throw new ServiceAssertionError(
        `Expected a single active sync config, got ${syncConfigs.map((config) => config._id.toHexString()).join(', ')}`
      );
    }
    const syncConfig = syncConfigs[0];
    if (syncConfig == null || !syncConfig.snapshot_done) {
      return null;
    }
    return {
      checkpoint: syncConfig.last_checkpoint ?? 0n,
      lsn: syncConfig.last_checkpoint_lsn ?? null
    };
  }

  protected async terminateSyncRuleState(): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          state: storage.SyncRuleState.TERMINATED,
          persisted_lsn: null,
          sync_configs: []
        }
      }
    );
  }

  protected async getStatusImpl(): Promise<storage.ReplicationStreamStatus> {
    const doc = await this.syncRulesCollection.findOne(this.syncConfigMatch(), {
      projection: this.syncConfigProjection({ resume_lsn: 1 })
    });
    const syncConfigs = this.selectedSyncConfigs(doc);
    if (doc == null || syncConfigs.length == 0) {
      throw new ServiceAssertionError('Cannot find replication stream status');
    }

    return {
      snapshotDone:
        syncConfigs.every((config) => config.snapshot_done ?? false) &&
        syncConfigs.every((config) => config.last_checkpoint_lsn != null),
      resumeLsn: doc.resume_lsn ?? null
    };
  }

  protected async clearSyncRuleState(): Promise<void> {
    // Clearing resets the entire replication stream (bucket data and the op sequence), so reset
    // the checkpoint state for _all_ embedded sync configs, not only the ones selected for this
    // storage instance. This maintains the invariant that no config has a last_checkpoint past
    // the stream-level last_persisted_op.
    await this.syncRulesCollection.updateOne(
      { _id: this.replicationStreamId },
      {
        $set: {
          persisted_lsn: null,
          'sync_configs.$[].snapshot_done': false,
          'sync_configs.$[].last_checkpoint_lsn': null,
          'sync_configs.$[].last_checkpoint': null,
          'sync_configs.$[].no_checkpoint_before': null
        },
        $unset: {
          resume_lsn: 1,
          last_persisted_op: 1
        }
      },
      {
        maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS
      }
    );
  }

  /**
   * The persisted mapping of the single sync config that read operations are served from.
   *
   * Reads always operate on a single sync config: read-path storage instances are
   * constructed for the active sync config only (see MongoBucketStorage.getActiveSyncConfig),
   * and checkpoints are served from the single active config (see fetchCheckpointState).
   * Within a single sync config, unique names are the persistence key of its rule mapping,
   * so its name-keyed {@link SingleSyncConfigBucketDefinitionMapping} resolves sources from
   * any parse of that config unambiguously - no parsed-set identity is required.
   *
   * Throws on storage instances with multiple sync configs (replication-side instances),
   * which must not serve reads.
   */
  private singleSyncConfigMapping(): SingleSyncConfigBucketDefinitionMapping {
    const content = this.replicationStream.syncConfigContent;
    if (content.length != 1) {
      throw new ServiceAssertionError(
        `Read operations require a storage instance with a single sync config, got ${content.length}`
      );
    }
    return content[0].mapping;
  }

  protected get versionContext(): MongoSyncBucketStorageContextV3 {
    const self = this;
    return {
      db: this.db,
      replicationStreamId: this.replicationStreamId,
      readPreference: this.readPreference,
      get mapping() {
        return self.singleSyncConfigMapping();
      }
    };
  }

  protected getParameterSetsImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    lookups: ScopedParameterLookup[],
    limit: number
  ): Promise<ParameterLookupRows[]> {
    return getParameterSetsV3(this.versionContext, checkpoint, lookups, limit);
  }

  protected getBucketDataBatchImpl(
    checkpoint: MongoSyncBucketStorageCheckpoint,
    dataBuckets: storage.BucketDataRequest[],
    options?: storage.BucketDataBatchOptions
  ): AsyncIterable<storage.SyncBucketDataChunk> {
    return getBucketDataBatchV3(this.versionContext, checkpoint, dataBuckets, options);
  }

  protected async clearBucketData(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listBucketDataCollections(this.replicationStreamId)) {
      await collection.drop();
    }
  }

  protected async clearParameterIndexes(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listParameterIndexCollections(this.replicationStreamId)) {
      await collection.collection.drop();
    }
  }

  protected async clearSourceRecords(_signal?: AbortSignal): Promise<void> {
    for (const collection of await this.db.listSourceRecordCollections(this.replicationStreamId)) {
      await collection.drop();
    }
  }

  protected async clearBucketState(_signal?: AbortSignal): Promise<void> {
    await this.db
      .bucketState(this.replicationStreamId)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  protected async clearSourceTables(_signal?: AbortSignal): Promise<void> {
    await this.db
      .sourceTables(this.replicationStreamId)
      .drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS })
      .catch((error) => {
        if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
          return;
        }
        throw error;
      });
  }

  async cleanupStoppedSyncConfigs(
    options: storage.CleanupStoppedSyncConfigsOptions
  ): Promise<storage.CleanupStoppedSyncConfigsResult> {
    return new MongoStoppedSyncConfigCleanup({
      db: this.db,
      replicationStreamId: this.replicationStreamId,
      signal: options.signal,
      logger: options.logger ?? this.logger,
      defaultSchema: options.defaultSchema,
      sourceConnectionTag: options.sourceConnectionTag
    }).run();
  }

  protected getDataBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
    return getDataBucketChangesV3(this.versionContext, options);
  }

  protected getParameterBucketChangesImpl(
    options: GetCheckpointChangesOptions
  ): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
    return getParameterBucketChangesV3(this.versionContext, options);
  }
}

export async function getParameterSetsV3(
  ctx: MongoSyncBucketStorageContextV3,
  checkpoint: MongoSyncBucketStorageCheckpoint,
  lookups: ScopedParameterLookup[],
  limit: number
): Promise<ParameterLookupRows[]> {
  return ctx.db.client.withSession({ snapshot: true }, async (session) => {
    setSessionSnapshotTime(session, checkpoint.snapshotTime);

    const buildLookupPipeline = (
      lookup: ScopedParameterLookup,
      index: number
    ): {
      collection: mongo.Collection<BucketParameterDocumentV3>;
      pipeline: mongo.Document[];
    } => {
      const indexId = lookup.indexId;
      const collection = ctx.db.parameterIndex(ctx.replicationStreamId, indexId);
      const lookupFilter = serializeParameterLookup(lookup);

      return {
        collection,
        pipeline: [
          {
            $match: {
              lookup: lookupFilter,
              _id: { $lte: checkpoint.checkpoint }
            }
          },
          {
            $sort: {
              key: 1,
              _id: -1
            }
          },
          {
            $group: {
              _id: {
                key: '$key'
              },
              bucket_parameters: {
                $first: '$bucket_parameters'
              }
            }
          },
          {
            $project: {
              _id: 0,
              bucket_parameters: 1,
              index: { $literal: index }
            }
          }
        ]
      };
    };

    const [firstLookup, ...remainingLookups] = lookups;
    const firstQuery = firstLookup == null ? null : buildLookupPipeline(firstLookup, 0);
    if (firstQuery == null) {
      return [];
    }

    const pipeline: mongo.Document[] = [
      ...firstQuery.pipeline,
      ...remainingLookups.map((lookup, indexInRemaining) => {
        const query = buildLookupPipeline(lookup, indexInRemaining + 1);
        return {
          $unionWith: {
            coll: query.collection.collectionName,
            pipeline: query.pipeline
          }
        };
      }),
      { $unwind: '$bucket_parameters' },
      { $limit: limit + 1 }
    ];

    const rows = await firstQuery.collection
      .aggregate<{ index: number; bucket_parameters: SqliteJsonRow }>(pipeline, {
        session,
        readConcern: 'snapshot',
        maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
      })
      .toArray()
      .catch((e) => {
        throw lib_mongo.mapQueryError(e, 'while evaluating parameter queries');
      });

    if (rows.length > limit) {
      throw new ParameterSetLimitExceededError(limit);
    }

    const byLookup = Map.groupBy(rows, (row) => lookups[row.index]);

    const results: ParameterLookupRows[] = [];
    byLookup.forEach((value, lookup) => results.push({ lookup, rows: value.map((r) => r.bucket_parameters) }));
    return results;
  });
}

export async function* getBucketDataBatchV3(
  ctx: MongoSyncBucketStorageContextV3,
  checkpoint: MongoSyncBucketStorageCheckpoint,
  dataBuckets: storage.BucketDataRequest[],
  options?: storage.BucketDataBatchOptions
): AsyncIterable<storage.SyncBucketDataChunk> {
  if (dataBuckets.length == 0) {
    return;
  }

  if (checkpoint.checkpoint == null) {
    throw new Error('checkpoint is null');
  }

  const readPreference = options?.requestHint == 'bulk' ? ctx.readPreference : undefined;
  const readConcern = ctx.readPreference == null ? undefined : 'majority';
  const session =
    readPreference == null || checkpoint.snapshotTime == null
      ? undefined
      : ctx.db.client.startSession({ causalConsistency: true });
  await using _ = { [Symbol.asyncDispose]: async () => session?.endSession() };

  if (session != null) {
    session.advanceOperationTime(checkpoint.snapshotTime);
    session.advanceClusterTime(checkpoint.clusterTime);
  }

  const batchLimit = options?.limit ?? storage.DEFAULT_DOCUMENT_BATCH_LIMIT;
  const chunkSizeLimitBytes = options?.chunkLimitBytes ?? storage.DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES;
  const end = checkpoint.checkpoint;
  let remainingLimit = batchLimit;

  const requestsByDefinition = new Map<string, storage.BucketDataRequest[]>();
  for (const request of dataBuckets) {
    const definitionId = ctx.mapping.bucketSourceId(request.source);
    const requests = requestsByDefinition.get(definitionId) ?? [];
    requests.push(request);
    requestsByDefinition.set(definitionId, requests);
  }

  const definitionGroups = Array.from(requestsByDefinition.entries());
  for (const [groupIndex, [definitionId, requests]] of definitionGroups.entries()) {
    if (remainingLimit <= 0) {
      break;
    }
    const hasLaterDefinitionGroups = groupIndex < definitionGroups.length - 1;
    const bucketMap = new Map(requests.map((request) => [request.bucket, request.start]));
    const filters = Array.from(bucketMap.entries()).map(([bucket, start]) => ({
      _id: {
        $gt: { b: bucket, o: start },
        $lte: { b: bucket, o: new bson.MaxKey() }
      },
      min_op: { $lte: end }
      // MongoDB Filter<T> doesn't accept compound _id ranges or dotted field paths in its type.
    })) as unknown as mongo.Filter<BucketDataDocumentV3>[];

    const collection = ctx.db.bucketData(ctx.replicationStreamId, definitionId);
    // MongoDB Filter<T> doesn't accept the $or operator in its type.
    const filter = { $or: filters } as unknown as mongo.Filter<BucketDataDocumentV3>;
    const context = { replicationStreamId: ctx.replicationStreamId, definitionId };
    const limit = remainingLimit;

    const cursorOptions = { limit: remainingLimit, batchSize: remainingLimit + 1 };

    // raw: true returns Buffers, but the driver typing doesn't reflect that
    // without an explicit cast to FindCursor<Buffer>.
    const cursor = collection.find(filter, {
      session,
      readPreference,
      readConcern,
      sort: { _id: 1 },
      raw: true,
      maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS,
      ...cursorOptions
    }) as unknown as mongo.FindCursor<Buffer>;

    let { data: rawData, hasMore } = await readSingleBatch(cursor).catch((e: unknown) => {
      throw lib_mongo.mapQueryError(e, 'while reading bucket data');
    });

    if (cursorOptions.limit != null && rawData.length >= cursorOptions.limit) {
      hasMore = true;
    }

    const data: BucketDataDoc[] = [];
    const documentOpCounts: number[] = [];
    const documentSizes: number[] = [];
    let sharedRemainingLimit = limit;
    let limitReached = false;
    // Buckets whose matched document contributed no rows after filtering.
    const completeEmptyBuckets = new Set<string>();

    for (const raw of rawData) {
      const doc = bson.deserialize(raw, storage.BSON_DESERIALIZE_INTERNAL_OPTIONS) as BucketDataDocumentV3;
      const {
        rows,
        remainingLimit,
        limitReached: docLimitReached
      } = extractRowsFromDocument(doc, context, bucketMap, end, sharedRemainingLimit);
      if (rows.length == 0) {
        // The document straddles the requested (start, end] window: it matched the
        // query, but none of its ops are in range. Since its _id.o (max op) must be
        // > end (any op <= end would have been > start, and thus in range), and
        // document ranges per bucket are disjoint, no later document for this bucket
        // can match either. The bucket is complete through the checkpoint.
        completeEmptyBuckets.add(doc._id.b);
      }
      data.push(...rows);
      documentOpCounts.push(rows.length);
      documentSizes.push(raw.byteLength);
      sharedRemainingLimit = remainingLimit;
      if (docLimitReached) {
        limitReached = true;
        break;
      }
    }

    const batchHasMore = hasMore || limitReached;

    // Empty chunks are not forwarded to clients, but report progress to the caller:
    // the bucket's position advances to the checkpoint, so it is not re-requested.
    // If the batch produced no data at all, the last empty chunk also carries the
    // has_more signal, so the caller re-requests the remaining buckets instead of
    // treating an all-filtered batch as the end of the stream.
    const emptyBuckets = Array.from(completeEmptyBuckets);
    for (const [index, bucket] of emptyBuckets.entries()) {
      const startOpId = bucketMap.get(bucket);
      if (startOpId == null) {
        throw new ServiceAssertionError(`data for unexpected bucket: ${bucket}`);
      }
      const isLastChunkOfBatch = data.length == 0 && index == emptyBuckets.length - 1;
      yield {
        chunkData: {
          bucket,
          after: internalToExternalOpId(startOpId),
          has_more: isLastChunkOfBatch && batchHasMore,
          data: [],
          next_after: internalToExternalOpId(end)
        },
        targetOp: null
      };
    }

    if (data.length == 0) {
      if (batchHasMore) {
        // The remaining documents are read in the next round, after the caller has
        // advanced the positions of the empty buckets above.
        return;
      }
      continue;
    }

    remainingLimit -= data.length;

    let currentChunkSizeBytes = 0;
    let currentChunk: utils.SyncBucketData | null = null;
    let targetOp: InternalOpId | null = null;

    for (const { row, docIndex, isLastOpInDocument } of walkDocumentOps(data, documentOpCounts, documentSizes)) {
      const bucket = row.bucketKey.bucket;

      if (currentChunk == null || currentChunk.bucket != bucket || currentChunkSizeBytes >= chunkSizeLimitBytes) {
        let start: ProtocolOpId | undefined = undefined;
        if (currentChunk != null) {
          if (currentChunk.bucket == bucket) {
            currentChunk.has_more = true;
            start = currentChunk.next_after;
          }

          const yieldChunk = currentChunk;
          currentChunk = null;
          currentChunkSizeBytes = 0;
          yield { chunkData: yieldChunk, targetOp };
          targetOp = null;
        }

        if (start == null) {
          const startOpId = bucketMap.get(bucket);
          if (startOpId == null) {
            throw new Error(`data for unexpected bucket: ${bucket}`);
          }
          start = internalToExternalOpId(startOpId);
        }
        currentChunk = {
          bucket,
          after: start,
          has_more: false,
          data: [],
          next_after: start
        };
      }

      const entry = mapOpEntry(row);
      if (row.target_op != null && (targetOp == null || row.target_op > targetOp)) {
        targetOp = row.target_op;
      }

      currentChunk.data.push(entry);
      currentChunk.next_after = entry.op_id;

      if (isLastOpInDocument) {
        currentChunkSizeBytes += documentSizes[docIndex];
      }
    }

    if (currentChunk != null) {
      const yieldChunk = currentChunk;
      yieldChunk.has_more = batchHasMore || (remainingLimit <= 0 && hasLaterDefinitionGroups);
      yield { chunkData: yieldChunk, targetOp };
    }

    if (batchHasMore || remainingLimit <= 0) {
      return;
    }
  }
}

export async function getDataBucketChangesV3(
  ctx: MongoSyncBucketStorageContextV3,
  options: GetCheckpointChangesOptions
): Promise<Pick<CheckpointChanges, 'updatedDataBuckets' | 'invalidateDataBuckets'>> {
  const limit = 1000;
  const bucketStateUpdates = await ctx.db
    .bucketState(ctx.replicationStreamId)
    .aggregate<{ _id: string; last_op: bigint }>(
      [
        {
          $match: {
            last_op: { $gt: options.lastCheckpoint.checkpoint }
          }
        },
        {
          $group: {
            _id: '$_id.b',
            last_op: { $max: '$last_op' }
          }
        },
        {
          $sort: {
            last_op: 1
          }
        },
        {
          $limit: limit + 1
        }
      ],
      { maxTimeMS: lib_mongo.MONGO_CHECKSUM_TIMEOUT_MS }
    )
    .toArray();

  const buckets = bucketStateUpdates.map((doc) => doc._id);
  const invalidateDataBuckets = buckets.length > limit;

  return {
    invalidateDataBuckets,
    updatedDataBuckets: invalidateDataBuckets ? new Set<string>() : new Set(buckets)
  };
}

export async function getParameterBucketChangesV3(
  ctx: MongoSyncBucketStorageContextV3,
  options: GetCheckpointChangesOptions
): Promise<Pick<CheckpointChanges, 'updatedParameterLookups' | 'invalidateParameterBuckets'>> {
  const limit = 1000;
  const indexIds = ctx.mapping.allParameterIndexIds();
  const collections = indexIds.map((indexId) => ({
    indexId,
    collection: ctx.db.parameterIndex(ctx.replicationStreamId, indexId)
  }));
  if (collections.length == 0) {
    return {
      invalidateParameterBuckets: false,
      updatedParameterLookups: new Set<string>()
    };
  }
  const checkpointFilter = {
    _id: { $gt: options.lastCheckpoint.checkpoint, $lte: options.nextCheckpoint.checkpoint }
  };
  const pipelineForCollection = (indexId: string) => [
    {
      $match: checkpointFilter
    },
    {
      $project: {
        _id: 0,
        lookup: 1,
        indexId: { $literal: indexId }
      }
    }
  ];
  const [firstCollection, ...remainingCollections] = collections;
  const parameterUpdates = await firstCollection.collection
    .aggregate<{ lookup: bson.Binary; indexId: string }>(
      [
        ...pipelineForCollection(firstCollection.indexId),
        ...remainingCollections.map((collection) => {
          return {
            $unionWith: {
              coll: collection.collection.collectionName,
              pipeline: pipelineForCollection(collection.indexId)
            }
          };
        }),
        {
          $limit: limit + 1
        }
      ],
      {
        batchSize: limit + 2,
        maxTimeMS: lib_mongo.db.MONGO_OPERATION_TIMEOUT_MS
      }
    )
    .toArray();

  const invalidateParameterUpdates = parameterUpdates.length > limit;

  return {
    invalidateParameterBuckets: invalidateParameterUpdates,
    updatedParameterLookups: invalidateParameterUpdates
      ? new Set<string>()
      : new Set<string>(parameterUpdates.map((p) => JSONBig.stringify(deserializeParameterLookup(p.lookup, p.indexId))))
  };
}
