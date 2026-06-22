# Source Modules

This page compares the current replication modules. Each module provides the replication implementation and jobs for one source type, implements the shared contracts described in [source-connector-contract.md](./03-source-connector-contract.md), and writes through the storage contract described in [storage-writer-contract.md](./04-storage-writer-contract.md).

## Comparison

| Source     | Position                                  | Stream                                   | Snapshot approach                                                                      | Write checkpoint barrier                                                                     | History loss handling                                                                                     |
| ---------- | ----------------------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Postgres   | WAL LSN                                   | Logical replication slot with `pgoutput` | Creates/uses a slot, snapshots tables, then waits for WAL catch-up                     | Reads `pg_current_wal_lsn()` and sends a logical keepalive message                           | Missing or invalidated slot raises `MissingReplicationSlotError` and can restart replication              |
| MongoDB    | `MongoLSN` from cluster time/resume token | Change stream                            | Snapshotter queues collections and stores resume state                                 | Reads session cluster time and updates `_powersync_checkpoints`                              | Invalidated change stream raises `ChangeStreamInvalidatedError` and restarts replication                  |
| MySQL      | Comparable GTID/binlog position           | Binlog stream                            | Reads executed GTID, snapshots tables, then streams binlog events                      | Reads executed GTID; forcing a later replicated marker is still noted as TODO in the adapter | Binlog availability is checked during initial/resume paths; missing history requires re-snapshot behavior |
| SQL Server | CDC LSN                                   | CDC polling stream                       | Stores snapshot LSN, snapshots tables, then polls CDC changes                          | Reads latest LSN and writes to the PowerSync checkpoints table                               | Expired CDC data raises `CDCDataExpiredError` and restarts replication                                    |
| Convex     | Encoded Convex cursor                     | Streaming Export APIs                    | Pins or resumes a global snapshot cursor, paginates `list_snapshot`, then polls deltas | Reads head cursor and writes a `powersync_checkpoints` marker                                | Expired cursor raises `ConvexCursorExpiredError` and restarts replication                                 |

Schema change handling is also source-specific. A module may automatically react to table, collection, column, or replica identity changes during streaming, or it may require operators to deploy a new sync config after source schema changes. Source-specific documentation should state which model applies.

## Postgres

Primary files:

- [`WalStreamReplicator`](../../modules/module-postgres/src/replication/WalStreamReplicator.ts)
- [`WalStreamReplicationJob`](../../modules/module-postgres/src/replication/WalStreamReplicationJob.ts)
- [`WalStream`](../../modules/module-postgres/src/replication/WalStream.ts)
- [`PostgresRouteAPIAdapter`](../../modules/module-postgres/src/api/PostgresRouteAPIAdapter.ts)

Postgres uses logical replication slots and WAL LSNs. The replication slot preserves WAL history for the stream, so `setResumeLsn()` is generally not needed for streaming progress.

Initial replication snapshots selected tables and records boundaries that streaming must pass before checkpoints become valid. The deeper design history is in [initial-replication.md](../modules/postgres/initial-replication.md).

The route adapter creates managed write checkpoint heads by reading `pg_current_wal_lsn()` and then sending a logical keepalive message.

## MongoDB

Primary files:

- [`ChangeStreamReplicator`](../../modules/module-mongodb/src/replication/ChangeStreamReplicator.ts)
- [`ChangeStreamReplicationJob`](../../modules/module-mongodb/src/replication/ChangeStreamReplicationJob.ts)
- [`ChangeStream`](../../modules/module-mongodb/src/replication/ChangeStream.ts)
- [`MongoSnapshotter`](../../modules/module-mongodb/src/replication/MongoSnapshotter.ts)
- [`MongoRouteAPIAdapter`](../../modules/module-mongodb/src/api/MongoRouteAPIAdapter.ts)

MongoDB uses change streams and stores resume state in bucket storage. Initial snapshots and streaming can overlap depending on snapshotter support.

The route adapter reads session cluster time for the managed write checkpoint head, then updates the checkpoints collection so the change stream observes a later operation.

If the change stream is invalidated or the resume token is no longer available, the module restarts replication with a fresh stream.

## MySQL

Primary files:

- [`BinLogReplicator`](../../modules/module-mysql/src/replication/BinLogReplicator.ts)
- [`BinLogReplicationJob`](../../modules/module-mysql/src/replication/BinLogReplicationJob.ts)
- [`BinLogStream`](../../modules/module-mysql/src/replication/BinLogStream.ts)
- [`MySQLRouteAPIAdapter`](../../modules/module-mysql/src/api/MySQLRouteAPIAdapter.ts)

MySQL uses GTID/binlog positions. It persists resume state in bucket storage and checks whether required binlog files are still available when resuming.

Keepalives are handled by the binlog heartbeat mechanism. The route adapter currently reads the executed GTID for write checkpoints and contains a TODO to ensure another message is replicated.

## SQL Server

Primary files:

- [`CDCReplicator`](../../modules/module-mssql/src/replication/CDCReplicator.ts)
- [`CDCReplicationJob`](../../modules/module-mssql/src/replication/CDCReplicationJob.ts)
- [`CDCStream`](../../modules/module-mssql/src/replication/CDCStream.ts)
- [`MSSQLRouteAPIAdapter`](../../modules/module-mssql/src/api/MSSQLRouteAPIAdapter.ts)

SQL Server uses CDC LSNs and polling. The stream stores a snapshot LSN, snapshots selected tables, and then polls CDC changes from the persisted resume position.

The route adapter reads the latest LSN and writes to the PowerSync checkpoints table so CDC captures a later marker. If CDC history has expired, the module restarts replication.

## Convex

Primary files:

- [`ConvexReplicator`](../../modules/module-convex/src/replication/ConvexReplicator.ts)
- [`ConvexReplicationJob`](../../modules/module-convex/src/replication/ConvexReplicationJob.ts)
- [`ConvexStream`](../../modules/module-convex/src/replication/ConvexStream.ts)
- [`ConvexRouteAPIAdapter`](../../modules/module-convex/src/api/ConvexRouteAPIAdapter.ts)

Convex uses Streaming Export APIs. Initial replication pins or reuses a global snapshot cursor and paginates table snapshots. Streaming then consumes document deltas.

Convex tables use `_id` as the replication identity and deltas contain full document state. Convex-specific edge cases are documented in:

- [snapshot-consistency.md](../modules/convex/snapshot-consistency.md)
- [schema-change-handling.md](../modules/convex/schema-change-handling.md)
- [convex-write-checkpoints.md](../modules/convex/convex-write-checkpoints.md)
