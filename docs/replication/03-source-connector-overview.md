# Source Connector Overview

This page describes the high-level responsibilities a replication module must satisfy for a source database or source API.

## Module Registration

A replication module extends `ReplicationModule<TConfig>`. It provides the source `type`, config codec, route API adapter, replicator factory, connection checks, and any source-specific setup needed after registration.

Only the first configured connection of a given source type is currently used.

## Config Type And Normalization

PowerSync service config stores source connections as generic data source configs. A replication module narrows that generic shape by extending `DataSourceConfig` with a unique literal `type` field and source-specific connection details.

The common pattern is:

```ts
const ExampleConnectionConfig = DataSourceConfig.and(
  t.object({
    type: t.literal(EXAMPLE_CONNECTION_TYPE),
    uri: t.string.optional(),
    hostname: t.string.optional(),
    port: portCodec.optional()
    // source-specific fields...
  })
);
```

`ReplicationModule` uses the `type` field to select the matching connection entry, validates it with the module codec, and decodes it before creating route APIs or replicators.

Most modules then define a resolved config type by combining the decoded input with normalized fields:

```ts
type ResolvedExampleConnectionConfig = ExampleConnectionConfig & NormalizedExampleConnectionConfig;
```

The usual `resolveConfig()` pattern is:

```ts
return {
  ...config,
  ...normalizeConnectionConfig(config)
};
```

Normalization handles the optional and alternate input forms that are convenient in YAML but awkward for runtime code. Common examples include:

- Parsing a `uri` and merging it with explicit `hostname`, `port`, `username`, `password`, `database`, or schema fields.
- Accepting ports as numbers or strings via `portCodec`, then resolving them to numbers.
- Filling defaults for optional fields such as `id`, `tag`, booleans, polling intervals, request timeouts, or replication-specific options.
- Validating source-specific protocols and required fields after optional forms have been combined.
- Resolving optional authentication variants into the concrete connection parameters used by the source client.
- Producing derived values such as base URIs, lookup functions, connection parameter objects, and normalized replication options.

After normalization, downstream code should use the resolved config type so route adapters, connection managers, replicators, and connection tests do not need to repeatedly handle every optional input shape.

## Route API Adapter

The `RouteAPI` adapter bridges API routes to source-specific capabilities.

The most replication-sensitive methods are:

```ts
getReplicationHead(): Promise<string>
advanceReplicationHead(head: string): Promise<void>
```

For managed write checkpoints, the source adapter must:

1. Read the current source replication head.
2. Let storage persist any write-checkpoint mapping for that head.
3. If storage actually advanced a write checkpoint, ensure that the replication stream will later observe the head or a greater source position.

Step 3 is important for managed write checkpoints. If the source database is idle, the mapping can be stored correctly but never become visible to a connected client unless replication observes a later checkpoint update. If storage reports that no managed checkpoint advanced, the caller should skip the source marker.

Current source examples:

- Postgres reads `pg_current_wal_lsn()` and writes a logical keepalive message.
- MongoDB reads session cluster time and updates the `_powersync_checkpoints` collection.
- SQL Server reads the latest LSN and updates the PowerSync checkpoints table.
- Convex reads the head cursor and writes a checkpoint marker.
- MySQL currently reads the executed GTID and calls the callback; the adapter notes a TODO for forcing a later replicated message.

The adapter also exposes source schema or source metadata information in service-friendly formats. Admin, validation, diagnostics, and schema-generation paths use that metadata to understand which source tables or collections exist, which columns or fields are available, and what source defaults apply when sync config references an unqualified table name.

Schemaless or partially schema-aware sources may not be able to provide strict column and type information. In that case, the adapter should return the best available table or collection metadata, expose useful warnings, and document which validation or client-schema-generation features are limited.

The extracted schema or metadata is also used by tooling that generates client-side schemas from sync config. Each source adapter is responsible for translating source-specific metadata into the common table and column formats expected by these validation and generation paths where those formats can be populated.

The adapter also provides connection status, replication lag in bytes where available, source query execution for admin/debug use, and graceful shutdown.

## Source Stream Requirements

A source database or source API must provide a replication stream, CDC feed, operation log, export cursor, polling boundary, or equivalent mechanism with positional information.

The stream position does not need to be a database-native LSN. It can be a WAL LSN, GTID, CDC LSN, change stream resume token, cluster time, export cursor, synthetic marker position, or another source-specific token. The connector must be able to use that position for the guarantees it claims:

- Resume from a specific point, or rely on source-managed state that resumes from the correct point.
- Record a source boundary before or at initial snapshot start.
- Consume all source changes relevant to PowerSync between that boundary and a later checkpoint.
- Compare source heads for managed write checkpoint acknowledgement when managed write checkpoints are supported.
- Detect when required source history has expired or become unavailable.

If the source stream is filtered, the connector must account for changes that can advance the source head without appearing in the stream. A managed write checkpoint stored at such a head can stall forever unless the connector can force a later event that is guaranteed to appear in the replication stream.

Resume state ownership must be explicit. Some sources keep the restart cursor in source-side state, such as a logical replication slot, and reconnecting to that source-side state resumes the stream. Other sources require PowerSync to persist a resume token in bucket storage and later read it back on restart. That persisted resume token is a restart cursor; it is not a visible checkpoint unless the stream also advances the committed source position at a valid checkpoint or keepalive boundary.

When a source has no suitable positional stream, it is not a good fit for a normal replication module. It may need a custom source-side marker table, an operation-log export, a polling table with monotonic positions, or a source API feature before it can meet the replication contract.

## Replicator

A replication module provides an `AbstractReplicator` subclass. The replicator owns the source-specific lifecycle around storage streams: starting per-stream jobs, cleaning up source-side state for stopped streams, and reporting connection health.

The replicator should not apply source changes directly. It delegates per-stream replication to jobs.

## Replication Job

A replication module provides an `AbstractReplicationJob` subclass. Each job owns one locked storage stream attempt: copying initial source data when needed, running ongoing streaming, keeping source-side state or connections healthy when required, and reporting replication lag for metrics when the source can measure it.

Jobs should treat `abortController.signal` as authoritative. When stopped, they should exit promptly and let the base class release the replication lock.

## Stream Helper

Most connectors put source-specific mechanics in a stream helper:

- Postgres: `WalStream`.
- MongoDB: `ChangeStream`.
- MySQL: `BinLogStream`.
- SQL Server: `CDCStream`.
- Convex: `ConvexStream`.

The helper owns the source replication workflow for a job attempt. It generally does:

- Initial replication and snapshot resume logic.
- Transition from initial replication to ongoing streaming.
- Source change parsing.
- Relation/table metadata discovery when the source module supports it during streaming.
- Writes source changes and source table metadata through the storage writer.
- Advances checkpoint and resume positions at source-safe boundaries.
- Source-specific replication lag calculation.

## Source History Loss

If the source can no longer resume from the persisted position, the connector must restart replication by requesting a fresh replication stream.

Examples:

- Postgres replication slot missing or invalidated.
- MongoDB change stream resume token invalidated.
- SQL Server CDC data expired.
- Convex cursor expired.

Restarting creates a new replication stream from the current sync config. This preserves correctness by forcing a fresh snapshot instead of attempting to continue from missing history.

## Keepalive Responsibilities

Keepalive behavior is source-specific:

- Some sources need an explicit source write or logical message so a stream position advances when idle.
- Some sources have protocol-level heartbeats.
- Some sources poll continuously and do not need a dedicated job keepalive.

When a keepalive creates no user data, the stream should advance the stored source position without writing bucket operations.

## Table Discovery And Source Metadata

Source connectors must produce `SourceEntityDescriptor` values for discovered tables or collections. The descriptor should include stable source identity, replica identity columns, and whether updates contain complete row images.

Connectors should run source table resolution whenever a source entity is discovered during snapshot or streaming. Storage then decides which `SourceTable` records should receive replicated data and which old records must be dropped.

Automatic schema change detection during streaming is optional. Some sources can observe relation, table, collection, or replica identity changes in the same stream as row changes. Other sources may only refresh schema metadata during initial replication or explicit sync config deployment.

If a module does not detect schema changes automatically, its source documentation must say so. Operators must then deploy a new sync config after changing source schema that affects sync rules, replicated columns, table selection, or replica identity. That deploy causes source metadata to be extracted again and, in most cases, creates initial replication work for the affected tables or collections.
