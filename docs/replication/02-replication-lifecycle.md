# Replication Lifecycle

This page describes the shared lifecycle implemented by `packages/service-core/src/replication`.

## Startup

The stream worker creates a `ReplicationEngine` and registers it with the service lifecycle. Replication modules are initialized by the module manager. If the service configuration contains a matching data source, each replication module:

1. Decodes and validates its data source config.
2. Registers its source-specific `AbstractReplicator` with `ReplicationEngine`.
3. Registers its `RouteAPI` adapter with the router engine.

When the service lifecycle starts, `ReplicationEngine.start()` starts every registered replicator. Setup that is specific to running replication belongs in the replicator's `start()` method; connection checks call `testConnection()` without starting replication.

## Replicator Loop

An `AbstractReplicator` is responsible for source-level orchestration. It does not apply individual source rows itself. Instead, it:

1. Uses `ServiceContext.syncConfigEngine` to load, parse, and validate the configured input snapshot after source adapters are registered.
2. Persists accepted changes through `BucketStorageFactory.configureSyncRules()`. With `exit_on_error: false`, fatal parser or source-capability errors are logged, pending processing is stopped, and the rejected config is not persisted. With `exit_on_error: true`, rejection stops startup before storage changes. Warnings are advisory.
3. Repeatedly refreshes the set of replication streams that should be running.
4. Acquires a replication lock for new streams.
5. Creates a source-specific `AbstractReplicationJob` with the locked `SyncRulesBucketStorage`.
6. Starts the job, which delegates initialization, snapshots, and streaming to its source-specific stream helper.
7. Stops jobs whose replication streams no longer need processing.
8. Cleans up stopped replication streams by calling source cleanup and then storage termination.

A rejected startup submission stops pending processing through `BucketStorageFactory.stopProcessingSyncConfigs()`. This includes processing configs written by older services, whether separate from or embedded alongside the active config. The active config serves clients and receives source changes.

In incremental storage, changing the runnable config set changes the replication job id; `refresh` stops the previous job before starting a replacement job for only the active config, using the same stored progress. In this rejection path, that matters when invalid configured YAML is submitted while a replacement config is still `PROCESSING` alongside an `ACTIVE` config. Cancelling the processing config leaves the active config to replicate on its own. Cancellation and activation use backend atomic operations so a config that has already become active is preserved.

Config submission and rejection cancellation run once at startup. On each refresh, runners with configured YAML check ownership of processing configs using the YAML and version label. They leave unrelated processing configs to a runner with matching input.

Starting another pod with older YAML can submit that YAML as a new deployment. A runner with rejected configured input does not cancel deployments during refresh. If an active config shares a stream with a processing config that fails the ownership checks, that runner cannot process the shared stream. Stopping the other pod does not cause the runner to resubmit its configured YAML.

`SyncConfigEngine.getConfigured()` caches one source-aware result, including its in-flight promise, for the process lifetime. Resolved service configuration retains only the configured input source and options. The engine loads raw YAML on its first `getConfigured()` call, then applies source defaults and capability checks after module registration. Replication requests this result during startup; API-only instances request it when first needed. Replication startup, local diagnostics, and manual reprocess eligibility share the resulting parsed config and structured diagnostics. The service distinguishes absent input (API-managed mode), unreadable input, rejected input, and accepted input. Read failures cause startup to fail. Consumers do not reread or watch configured files. Configure API and replication processes with identical copies and restart both after changes; different versions or capabilities can produce different validation results.

Diagnostics projects rejected local input into `deploying_sync_rules`, honoring content inclusion, without a storage record or replication status. The result is available on API-only instances and independently of source connectivity. It takes precedence over stored pending diagnostics. An API instance without readable configured input cannot report that input's validation errors.

Both manual reprocess endpoints reject requests while any config is processing or the local configured config has fatal admission errors. Correct the sync config and restart affected processes before requesting manual reprocessing. Automatic recovery of the active config is supported: a worker with rejected input may process replacement work matching the active YAML and version label, even if the active config is errored or reprocessing selects another storage version. If rejected startup cancels pending recovery while the active config is already `ERRORED`, startup invokes the backend recovery path to recreate runnable replacement work. This also repairs an errored active config whose recovery was previously lost or superseded. Recovery does not replace the local rejected diagnostics.

In API-managed mode, there is no configured snapshot to reject. Replication and diagnostics use persisted configs. The deploy route shares the validation pipeline and rejects fatal submissions before storage mutation; it returns errors to the caller without cancelling accepted work or retaining a local rejected overlay. Replication jobs use storage-owned canonical parsed config sets and hydration state rather than the local configured snapshot.

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
