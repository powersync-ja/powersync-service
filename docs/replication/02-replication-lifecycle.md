# Replication Lifecycle

This page describes the shared lifecycle implemented by `packages/service-core/src/replication`.

## Startup

The stream worker creates a `ReplicationEngine` and registers it with the service lifecycle. Replication modules are initialized by the module manager. If the service configuration contains a matching data source, each replication module:

1. Decodes and validates its data source config.
2. Registers its source-specific `AbstractReplicator` with `ReplicationEngine`.
3. Registers its `RouteAPI` adapter with the router engine.
4. Runs any module-specific initialization.

When the service lifecycle starts, `ReplicationEngine.start()` starts every registered replicator.

## Replicator Loop

An `AbstractReplicator` is responsible for source-level orchestration. It does not apply individual source rows itself. Instead, it:

1. Loads sync config from the configured sync rules provider.
2. Persists changed sync config through `BucketStorageFactory.configureSyncRules()`.
3. Repeatedly refreshes the set of replication streams that should be running.
4. Acquires a replication lock for new streams.
5. Creates a source-specific `AbstractReplicationJob` with the locked `SyncRulesBucketStorage`.
6. Starts the job.
7. Stops jobs whose replication streams no longer need processing.
8. Cleans up stopped replication streams by calling source cleanup and then storage termination.

The refresh loop runs continuously until the service is stopped. It also periodically calls `keepAlive()` on running jobs when the rate limiter allows it.

## Replication Stream States

The storage layer exposes streams in these states:

- `PROCESSING`: a new or deploying stream needs replication work, typically including initial snapshot.
- `ACTIVE`: the stream is complete enough for clients to sync from it and should continue streaming changes.
- `STOP`: the stream has been replaced or should no longer replicate.
- `TERMINATED`: the stream has been stopped and its storage has been cleared.
- `ERRORED`: the stream hit a permanent replication error but remains the stream clients see until replaced.

The replicator starts jobs for streams returned by `getReplicatingReplicationStreams()`. It separately terminates streams returned by `getStoppedReplicationStreams()`.

In storage modes with embedded sync config state, the top-level stream state and the per-config states are related but not identical. MongoDB storage v3 can keep the stream itself `ACTIVE` while an embedded sync config is `PROCESSING`; the sync API continues reading the active config while the replication job processes both active and deploying work through the same source stream.

A sync config deployment is also the manual refresh path for replication modules that do not automatically detect source schema changes while streaming. After a source table, collection, column set, or replica identity changes, those modules require a new sync config deploy so source metadata can be re-discovered. Depending on compatibility and storage support, that deploy either creates a separate `PROCESSING` stream or appends a processing config to the active stream, then snapshots the affected data before activation.

## Job Lifecycle

An `AbstractReplicationJob` owns a single stream-specific replication run.

A job receives:

- A stream id.
- A `SyncRulesBucketStorage` instance.
- A replication lock.
- A metrics engine.
- A source-specific error rate limiter.

`start()` calls the source-specific `replicate()` method. When `replicate()` exits, the job aborts its controller and releases the storage lock. `stop()` aborts the job and waits for the replication promise to settle.

Source-specific jobs usually follow this shape:

1. Create fresh source connections for the attempt.
2. Wait for retry backoff if a previous error was reported.
3. Create a stream helper such as `WalStream`, `ChangeStream`, `BinLogStream`, `CDCStream`, or `ConvexStream`.
4. Run that stream helper until it exits.
5. Report recoverable errors to the rate limiter.
6. Request `restartReplication()` for source-history-loss conditions that require a new replication stream.

The job does not implement initial replication or change application directly. That behavior lives in the source-specific stream helper created by `replicate()`.

## Locks And Multi-Process Safety

Storage locking is the shared coordination point for multi-process deployments. A job is only started after its stream lock has been acquired. If another process owns the lock, the replicator leaves the stream alone and tries again on a later refresh.

The lock is released by `AbstractReplicationJob` after replication exits, even on error. This allows another refresh cycle, or another process, to retry the stream.

## Cleanup

When a replication stream is stopped, the replicator calls `terminateSyncRules()`:

1. `cleanUp(syncRuleStorage)` lets the source module delete source-side state such as a Postgres replication slot.
2. `syncRuleStorage.terminate({ clearStorage: true })` clears bucket storage and marks the stream terminated.

Source cleanup must be idempotent. It should be safe to call even if source-side state has already been removed.

Incremental streams can also have stopped sync configs inside a still-running stream. In that case the source job may ask storage to clean up stopped embedded configs while streaming continues. Storage compares the stopped configs with live configs, drops only bucket data, parameter index, bucket state, source record, and source table state that no live config still uses, then prunes the stopped config state from the stream.

## Lag Reporting

The active replication job reports lag through `getReplicationLagMillis()`. The metric represents the approximate time between source commit time and PowerSync processing. The source stream implementation owns the exact calculation because source databases expose different metadata.
