import * as lib_postgres from '@powersync/lib-service-postgres';
import {
  container,
  logger as defaultLogger,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import {
  BucketStorageBatch,
  getUuidReplicaIdentityBson,
  MetricsEngine,
  RelationCache,
  SaveUpdate,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import {
  applyValueContext,
  CompatibilityContext,
  SqliteInputRow,
  SqliteInputValue,
  SqliteRow,
  SqlSyncRules,
  ToastableSqliteRow
} from '@powersync/service-sync-rules';

import { ReplicationMetric } from '@powersync/service-types';
import { PgManager } from './PgManager.js';
import { getPgOutputRelation, getRelId, referencedColumnTypeIds } from './PgRelation.js';
import { PostgresSnapshotter } from './PostgresSnapshotter.js';
import { ensureStorageCompatibility } from './replication-utils.js';
import { IdSnapshotQuery, MissingRow, PrimaryKeyValue } from './SnapshotQuery.js';

export interface WalStreamOptions {
  logger?: Logger;
  connections: PgManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abort_signal: AbortSignal;

  /**
   * Override snapshot chunk length (number of rows), for testing.
   *
   * Defaults to 10_000.
   *
   * Note that queries are streamed, so we don't actually keep that much data in memory.
   */
  snapshotChunkLength?: number;
}

export const ZERO_LSN = '00000000/00000000';
export const PUBLICATION_NAME = 'powersync';
export const POSTGRES_DEFAULT_SCHEMA = 'public';

export const KEEPALIVE_CONTENT = 'ping';
export const KEEPALIVE_BUFFER = Buffer.from(KEEPALIVE_CONTENT);
export const KEEPALIVE_STATEMENT: pgwire.Statement = {
  statement: /* sql */ `
    SELECT
      *
    FROM
      pg_logical_emit_message(FALSE, 'powersync', $1)
  `,
  params: [{ type: 'varchar', value: KEEPALIVE_CONTENT }]
} as const;

export const isKeepAliveMessage = (msg: pgwire.PgoutputMessage) => {
  return (
    msg.tag == 'message' &&
    msg.prefix == 'powersync' &&
    msg.content &&
    Buffer.from(msg.content).equals(KEEPALIVE_BUFFER)
  );
};

export const sendKeepAlive = async (db: pgwire.PgClient) => {
  await lib_postgres.retriedQuery(db, KEEPALIVE_STATEMENT);
};

export class MissingReplicationSlotError extends Error {
  constructor(message: string, cause?: any) {
    super(message);

    this.cause = cause;
  }
}

export class WalStream {
  sync_rules: SqlSyncRules;
  group_id: number;

  connection_id = 1;

  private logger: Logger;

  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly metrics: MetricsEngine;
  private readonly slot_name: string;

  private connections: PgManager;

  private abortController = new AbortController();
  private abortSignal: AbortSignal = this.abortController.signal;

  private initPromise: Promise<void> | null = null;
  private snapshotter: PostgresSnapshotter;

  private relationCache = new RelationCache((relation: number | SourceTable) => {
    if (typeof relation == 'number') {
      return relation;
    }
    return relation.objectId!;
  });

  private startedStreaming = false;

  /**
   * Time of the oldest uncommitted change, according to the source db.
   * This is used to determine the replication lag.
   */
  private oldestUncommittedChange: Date | null = null;
  /**
   * Keep track of whether we have done a commit or keepalive yet.
   * We can only compute replication lag if isStartingReplication == false, or oldestUncommittedChange is present.
   */
  private isStartingReplication = true;

  constructor(private options: WalStreamOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.sync_rules = options.storage.getParsedSyncRules({ defaultSchema: POSTGRES_DEFAULT_SCHEMA });
    this.group_id = options.storage.group_id;
    this.slot_name = options.storage.slot_name;
    this.connections = options.connections;

    // We wrap in our own abort controller so we can trigger abort internally.
    options.abort_signal.addEventListener('abort', () => {
      this.abortController.abort();
    });
    if (options.abort_signal.aborted) {
      this.abortController.abort();
    }

    this.snapshotter = new PostgresSnapshotter({ ...options, abort_signal: this.abortSignal });
    this.abortSignal.addEventListener(
      'abort',
      () => {
        if (this.startedStreaming) {
          // Ping to speed up cancellation of streaming replication
          // We're not using pg_snapshot here, since it could be in the middle of
          // an initial replication transaction.
          const promise = sendKeepAlive(this.connections.pool);
          promise.catch((e) => {
            // Failures here are okay - this only speeds up stopping the process.
            this.logger.warn('Failed to ping connection', e);
          });
        } else {
          // If we haven't started streaming yet, it could be due to something like
          // and invalid password. In that case, don't attempt to ping.
        }
      },
      { once: true }
    );
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  async handleRelation(options: {
    batch: storage.BucketStorageBatch;
    descriptor: SourceEntityDescriptor;
    snapshot: boolean;
    referencedTypeIds: number[];
  }) {
    const { batch, descriptor, snapshot, referencedTypeIds } = options;

    if (!descriptor.objectId && typeof descriptor.objectId != 'number') {
      throw new ReplicationAssertionError(`objectId expected, got ${typeof descriptor.objectId}`);
    }
    const result = await this.storage.resolveTable({
      group_id: this.group_id,
      connection_id: this.connection_id,
      connection_tag: this.connections.connectionTag,
      entity_descriptor: descriptor,
      sync_rules: this.sync_rules
    });
    this.relationCache.update(result.table);

    // Drop conflicting tables. This includes for example renamed tables.
    if (result.dropTables.length > 0) {
      this.logger.info(`Dropping conflicting tables: ${result.dropTables.map((t) => t.qualifiedName).join(', ')}`);
      await batch.drop(result.dropTables);
    }

    // Ensure we have a description for custom types referenced in the table.
    await this.connections.types.fetchTypes(referencedTypeIds);

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync rules.
    const shouldSnapshot = snapshot && !result.table.snapshotComplete && result.table.syncAny;

    if (shouldSnapshot) {
      this.logger.info(`Queuing snapshot for new table ${result.table.qualifiedName}`);
      await this.snapshotter.queueSnapshot(batch, result.table);
    }

    return result.table;
  }

  /**
   * Process rows that have missing TOAST values.
   *
   * This can happen during edge cases in the chunked intial snapshot process.
   *
   * We handle this similar to an inline table snapshot, but limited to the specific
   * set of rows.
   */
  private async resnapshot(batch: BucketStorageBatch, rows: MissingRow[]) {
    const byTable = new Map<number, MissingRow[]>();
    for (let row of rows) {
      const relId = row.table.objectId as number; // always a number for postgres
      if (!byTable.has(relId)) {
        byTable.set(relId, []);
      }
      byTable.get(relId)!.push(row);
    }
    const db = await this.connections.snapshotConnection();
    try {
      for (let rows of byTable.values()) {
        const table = rows[0].table;
        await this.snapshotter.snapshotTableInTx(
          batch,
          db,
          table,
          rows.map((r) => r.key)
        );
      }
      // Even with resnapshot, we need to wait until we get a new consistent checkpoint
      // after the snapshot, so we need to send a keepalive message.
      await sendKeepAlive(db);
    } finally {
      await db.end();
    }
  }

  private getTable(relationId: number): storage.SourceTable {
    const table = this.relationCache.get(relationId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new ReplicationAssertionError(`Missing relation cache for ${relationId}`);
    }
    return table;
  }

  private syncRulesRecord(row: SqliteInputRow): SqliteRow;
  private syncRulesRecord(row: SqliteInputRow | undefined): SqliteRow | undefined;

  private syncRulesRecord(row: SqliteInputRow | undefined): SqliteRow | undefined {
    if (row == null) {
      return undefined;
    }
    return this.sync_rules.applyRowContext<never>(row);
  }

  private toastableSyncRulesRecord(row: ToastableSqliteRow<SqliteInputValue>): ToastableSqliteRow {
    return this.sync_rules.applyRowContext(row);
  }

  async writeChange(
    batch: storage.BucketStorageBatch,
    msg: pgwire.PgoutputMessage
  ): Promise<storage.FlushedResult | null> {
    if (msg.lsn == null) {
      return null;
    }
    if (msg.tag == 'insert' || msg.tag == 'update' || msg.tag == 'delete') {
      const table = this.getTable(getRelId(msg.relation));
      if (!table.syncAny) {
        this.logger.debug(`Table ${table.qualifiedName} not used in sync rules - skipping`);
        return null;
      }

      if (msg.tag == 'insert') {
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const baseRecord = this.syncRulesRecord(this.connections.types.constructAfterRecord(msg));
        return await batch.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: table,
          before: undefined,
          beforeReplicaId: undefined,
          after: baseRecord,
          afterReplicaId: getUuidReplicaIdentityBson(baseRecord, table.replicaIdColumns)
        });
      } else if (msg.tag == 'update') {
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        // "before" may be null if the replica id columns are unchanged
        // It's fine to treat that the same as an insert.
        const before = this.syncRulesRecord(this.connections.types.constructBeforeRecord(msg));
        const after = this.toastableSyncRulesRecord(this.connections.types.constructAfterRecord(msg));
        return await batch.save({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: table,
          before: before,
          beforeReplicaId: before ? getUuidReplicaIdentityBson(before, table.replicaIdColumns) : undefined,
          after: after,
          afterReplicaId: getUuidReplicaIdentityBson(after, table.replicaIdColumns)
        });
      } else if (msg.tag == 'delete') {
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        const before = this.syncRulesRecord(this.connections.types.constructBeforeRecord(msg)!);

        return await batch.save({
          tag: storage.SaveOperationTag.DELETE,
          sourceTable: table,
          before: before,
          beforeReplicaId: getUuidReplicaIdentityBson(before, table.replicaIdColumns),
          after: undefined,
          afterReplicaId: undefined
        });
      }
    } else if (msg.tag == 'truncate') {
      let tables: storage.SourceTable[] = [];
      for (let relation of msg.relations) {
        const table = this.getTable(getRelId(relation));
        tables.push(table);
      }
      return await batch.truncate(tables);
    }
    return null;
  }

  /**
   * Start replication loop, and continue until aborted or error.
   */
  async replicate() {
    let streamPromise: Promise<void> | null = null;
    let loopPromise: Promise<void> | null = null;
    try {
      this.initPromise = this.initReplication();
      await this.initPromise;
      // These Promises are both expected to run until aborted or error.
      streamPromise = this.streamChanges().finally(() => {
        this.abortController.abort();
      });
      loopPromise = this.snapshotter.replicationLoop().finally(() => {
        this.abortController.abort();
      });
      const results = await Promise.allSettled([loopPromise, streamPromise]);
      // First, prioritize non-aborted errors
      for (let result of results) {
        if (result.status == 'rejected' && !(result.reason instanceof ReplicationAbortedError)) {
          throw result.reason;
        }
      }
      // Then include aborted errors
      for (let result of results) {
        if (result.status == 'rejected') {
          throw result.reason;
        }
      }
      // If we get here, both Promises completed successfully, which is unexpected.
      throw new ReplicationAssertionError(`Replication loop exited unexpectedly`);
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    } finally {
      this.abortController.abort();
    }
  }

  public async waitForInitialSnapshot() {
    if (this.initPromise == null) {
      throw new ReplicationAssertionError('replicate() must be called before waitForInitialSnapshot()');
    }
    await this.initPromise;

    await this.snapshotter.waitForInitialSnapshot();
  }

  /**
   * Initialize replication.
   * Start replication loop, and continue until aborted, error or initial snapshot completed.
   */
  private async initReplication() {
    const result = await this.snapshotter.checkSlot();
    const db = await this.connections.snapshotConnection();
    try {
      await this.snapshotter.setupSlot(db, result);
      if (result.needsInitialSync) {
        await this.snapshotter.queueSnapshotTables(db);
      }
    } finally {
      await db.end();
    }
  }

  private async streamChanges() {
    const streamReplicationConnection = await this.connections.replicationConnection();
    try {
      await this.streamChangesInternal(streamReplicationConnection);
    } catch (e) {
      if (isReplicationSlotInvalidError(e)) {
        throw new MissingReplicationSlotError(e.message, e);
      }
      throw e;
    } finally {
      await streamReplicationConnection.end();
    }
  }

  private async streamChangesInternal(replicationConnection: pgwire.PgConnection) {
    // When changing any logic here, check /docs/wal-lsns.md.

    // Viewing the contents of logical messages emitted with `pg_logical_emit_message`
    // is only supported on Postgres >= 14.0.
    // https://www.postgresql.org/docs/14/protocol-logical-replication.html
    const { createEmptyCheckpoints, exposesLogicalMessages } = await ensureStorageCompatibility(
      this.connections.pool,
      this.storage.factory
    );

    const replicationOptions: Record<string, string> = {
      proto_version: '1',
      publication_names: PUBLICATION_NAME
    };
    if (exposesLogicalMessages) {
      /**
       * Only add this option if the Postgres server supports it.
       * Adding the option to a server that doesn't support it will throw an exception when starting logical replication.
       * Error: `unrecognized pgoutput option: messages`
       */
      replicationOptions['messages'] = 'true';
    }

    const replicationStream = replicationConnection.logicalReplication({
      slot: this.slot_name,
      options: replicationOptions
    });

    this.startedStreaming = true;

    let resnapshot: { table: storage.SourceTable; key: PrimaryKeyValue }[] = [];

    const markRecordUnavailable = (record: SaveUpdate) => {
      if (!IdSnapshotQuery.supports(record.sourceTable)) {
        // If it's not supported, it's also safe to ignore
        return;
      }
      let key: PrimaryKeyValue = {};
      for (let column of record.sourceTable.replicaIdColumns) {
        const name = column.name;
        const value = record.after[name];
        if (value == null) {
          // We don't expect this to actually happen.
          // The key should always be present in the "after" record.
          return;
        }
        // We just need a consistent representation of the primary key, and don't care about fixed quirks.
        key[name] = applyValueContext(value, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
      }
      resnapshot.push({
        table: record.sourceTable,
        key: key
      });
    };

    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ZERO_LSN,
        defaultSchema: POSTGRES_DEFAULT_SCHEMA,
        storeCurrentData: true,
        skipExistingRows: false,
        markRecordUnavailable
      },
      async (batch) => {
        // We don't handle any plain keepalive messages while we have transactions.
        // While we have transactions, we use that to advance the position.
        // Replication never starts in the middle of a transaction, so this starts as false.
        let skipKeepalive = false;
        let count = 0;

        for await (const chunk of replicationStream.pgoutputDecode()) {
          this.touch();

          if (this.abortSignal.aborted) {
            break;
          }

          // chunkLastLsn may come from normal messages in the chunk,
          // or from a PrimaryKeepalive message.
          const { messages, lastLsn: chunkLastLsn } = chunk;

          /**
           * We can check if an explicit keepalive was sent if `exposesLogicalMessages == true`.
           * If we can't check the logical messages, we should assume a keepalive if we
           * receive an empty array of messages in a replication event.
           */
          const assumeKeepAlive = !exposesLogicalMessages;
          let keepAliveDetected = false;
          const lastCommit = messages.findLast((msg) => msg.tag == 'commit');

          for (const msg of messages) {
            if (msg.tag == 'relation') {
              await this.handleRelation({
                batch,
                descriptor: getPgOutputRelation(msg),
                snapshot: true,
                referencedTypeIds: referencedColumnTypeIds(msg)
              });
            } else if (msg.tag == 'begin') {
              // This may span multiple transactions in the same chunk, or even across chunks.
              skipKeepalive = true;
              if (this.oldestUncommittedChange == null) {
                this.oldestUncommittedChange = new Date(Number(msg.commitTime / 1000n));
              }
            } else if (msg.tag == 'commit') {
              this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(1);
              if (msg == lastCommit) {
                // Only commit if this is the last commit in the chunk.
                // This effectively lets us batch multiple transactions within the same chunk
                // into a single flush, increasing throughput for many small transactions.
                skipKeepalive = false;
                // flush() must be before the resnapshot check - that is
                // typically what reports the resnapshot records.
                await batch.flush({ oldestUncommittedChange: this.oldestUncommittedChange });
                // This _must_ be checked after the flush(), and before
                // commit() or ack(). We never persist the resnapshot list,
                // so we have to process it before marking our progress.
                if (resnapshot.length > 0) {
                  await this.resnapshot(batch, resnapshot);
                  resnapshot = [];
                }
                const didCommit = await batch.commit(msg.lsn!, {
                  createEmptyCheckpoints,
                  oldestUncommittedChange: this.oldestUncommittedChange
                });
                await this.ack(msg.lsn!, replicationStream);
                if (didCommit) {
                  this.oldestUncommittedChange = null;
                  this.isStartingReplication = false;
                }
              }
            } else {
              if (count % 100 == 0) {
                this.logger.info(`Replicating op ${count} ${msg.lsn}`);
              }

              /**
               * If we can see the contents of logical messages, then we can check if a keepalive
               * message is present. We only perform a keepalive (below) if we explicitly detect a keepalive message.
               * If we can't see the contents of logical messages, then we should assume a keepalive is required
               * due to the default value of `assumeKeepalive`.
               */
              if (exposesLogicalMessages && isKeepAliveMessage(msg)) {
                keepAliveDetected = true;
              }

              count += 1;
              const flushResult = await this.writeChange(batch, msg);
              if (flushResult != null && resnapshot.length > 0) {
                // If we have large transactions, we also need to flush the resnapshot list
                // periodically.
                // TODO: make sure this bit is actually triggered
                await this.resnapshot(batch, resnapshot);
                resnapshot = [];
              }
            }
          }

          if (!skipKeepalive) {
            if (assumeKeepAlive || keepAliveDetected) {
              // Reset the detection flag.
              keepAliveDetected = false;

              // In a transaction, we ack and commit according to the transaction progress.
              // Outside transactions, we use the PrimaryKeepalive messages to advance progress.
              // Big caveat: This _must not_ be used to skip individual messages, since this LSN
              // may be in the middle of the next transaction.
              // It must only be used to associate checkpoints with LSNs.

              const didCommit = await batch.keepalive(chunkLastLsn);
              if (didCommit) {
                this.oldestUncommittedChange = null;
              }

              this.isStartingReplication = false;
            }

            // We receive chunks with empty messages often (about each second).
            // Acknowledging here progresses the slot past these and frees up resources.
            await this.ack(chunkLastLsn, replicationStream);
          }

          this.metrics.getCounter(ReplicationMetric.CHUNKS_REPLICATED).add(1);
        }
      }
    );
  }

  async ack(lsn: string, replicationStream: pgwire.ReplicationStream) {
    if (lsn == ZERO_LSN) {
      return;
    }

    replicationStream.ack(lsn);
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    if (this.oldestUncommittedChange == null) {
      if (this.isStartingReplication) {
        // We don't have anything to compute replication lag with yet.
        return undefined;
      } else {
        // We don't have any uncommitted changes, so replication is up-to-date.
        return 0;
      }
    }
    return Date.now() - this.oldestUncommittedChange.getTime();
  }

  private touch() {
    container.probes.touch().catch((e) => {
      this.logger.error(`Error touching probe`, e);
    });
  }
}

function isReplicationSlotInvalidError(e: any) {
  // We could access the error code from pgwire using this:
  //   e[Symbol.for('pg.ErrorCode')]
  // However, we typically get a generic code such as 42704 (undefined_object), which does not
  // help much. So we check the actual error message.
  const message = e.message ?? '';

  // Sample: record with incorrect prev-link 10000/10000 at 0/18AB778
  //   Seen during development. Some internal error, fixed by re-creating slot.
  //
  // Sample: publication "powersync" does not exist
  //   Happens when publication deleted or never created.
  //   Slot must be re-created in this case.
  return (
    /incorrect prev-link/.test(message) ||
    /replication slot.*does not exist/.test(message) ||
    /publication.*does not exist/.test(message) ||
    // Postgres 18 - exceeded max_slot_wal_keep_size
    /can no longer access replication slot/.test(message) ||
    // Postgres 17 - exceeded max_slot_wal_keep_size
    /can no longer get changes from replication slot/.test(message)
  );
}
