# MongoDB replication adapters

`ChangeStreamReplicator` accepts a `createReplicationQueryProvider` factory. It creates a provider for each
replication attempt, passing the hydrated sync config, connection tag and default database. The same provider
serves the attempt's snapshots, streaming reader, snapshot barriers and resume validation.

The default provider preserves ordinary MongoDB and DocumentDB replication:

```ts
const PROVIDER: MongoReplicationQueryProvider = {
  getSnapshotFilter: () => null,
  openChangeStream: ({ open }) => open({})
};
```

This repo defines the adapter boundary. The shared sync config parser validates MongoDB filter expression syntax.
A module that implements filtering owns source-capability validation, expression compilation, and decisions about
which documents belong in the filtered set.

## Snapshot selection

`getSnapshotFilter(table)` receives a resolved physical source table and returns an additional `find` predicate,
or `null` for ordinary snapshots. An aggregation expression can be supplied as `{ $expr: expression }`.
The snapshot query combines that predicate with its `_id` continuation using `$and`, so pagination does not
replace an existing `$expr` or `_id` condition. Filtered snapshots use simple collation; unfiltered snapshots
retain the existing collection-default behavior.

Filtered snapshots report an unknown total and a running replicated count, such as `3/?`. The service does not
run a separate filtered count query. Unfiltered snapshots retain the collection's estimated document count.

The provider can implement `validateSource({ connectionManager, isDocumentDb })` to reject unsupported source
capabilities before queries open. Both replication and the snapshotter call it, so implementations should permit
repeated validation. The default provider adds no capability requirements.

## Opening and adapting changes

`openChangeStream({ open })` receives a callback that opens the shared source reader. Its position, namespace
selection, post-image requirements, timeouts and cancellation are already bound. Providers can supply
`pipelineStages` and the `fullDocumentBeforeChange` image option. Additional stages run after namespace selection
and before MongoDB's final large-event split stage.

`open(...)` yields lazy parsed batches to both default and filtering providers. Raw transport batches and
split-event assembly remain inside the shared opener. Each batch
contains `events: Iterable<MongoReplicationStreamChange>`, `getResumeToken()` and `metadata`. Each change contains a complete
parsed `event` and `hasBufferedChanges`. Document bodies remain BSON buffers. Async iteration occurs once per
batch; synchronous iteration parses entries only as requested, without allocating an array of parsed envelopes.

The consumer contract is:

```ts
for await (const batch of stream) {
  for (const change of batch.events) {
    await processChange(change);
  }
  signal.throwIfAborted();
  const filteredCount = batch.metadata.filteredCount;
  // Aggregate non-null counts, including batches without a safe token.
  const resumeToken = batch.getResumeToken();
  if (resumeToken != null) {
    await flushPendingWrites();
    await persistResumeToken(resumeToken);
  }
}
```

Consume events once and completely before accessing the token, metadata or the next batch. Both token and
metadata access throw before exhaustion, including after the last event but before the iterator reports completion.
After exhaustion, `getResumeToken()` returns a stable safe token or `undefined`; `metadata` is a cached immutable
object. If processing fails or is cancelled, close the iterators without saving a token.

Requesting the next batch before the current one is exhausted is an error: the shared opener throws a `ReplicationAssertionError` on that next pull instead of silently skipping the batch. This applies to empty batches too, and to providers that stop iterating `batch.events` early. Closing the stream instead of pulling again is fine.

Filtering providers use `filterMongoReplicationStream` instead of constructing batch wrappers. This basic
example selects documents by an immutable string ID prefix, so snapshots and streaming use the same predicate:

```ts
import {
  filterMongoReplicationStream,
  MongoReplicationFilter,
  MongoReplicationFilterResult,
  MongoReplicationQueryProvider
} from '@powersync/service-module-mongodb';

const FILTER: MongoReplicationFilter = (event) =>
  String(event.documentKey._id).startsWith('public-')
    ? MongoReplicationFilterResult.KEEP
    : MongoReplicationFilterResult.EXCLUDE;

const PROVIDER: MongoReplicationQueryProvider = {
  getSnapshotFilter: () => ({ $expr: { $regexMatch: { input: '$_id', regex: '^public-' } } }),
  openChangeStream: ({ open }) => filterMongoReplicationStream({ batches: open({}), filter: FILTER })
};
```

The runnable [filtering example test](../../modules/module-mongodb/test/src/filtering_example.test.ts) imports the
public module entry point and verifies retained IDs, exclusion counts and resume progress through a fully filtered batch.
This example assumes string IDs. Filters on mutable fields must also handle documents leaving the selected set:
request the required pre-images through `open({ imageOptions: { fullDocumentBeforeChange: 'required' } })`, evaluate
before/after membership, and return `MongoReplicationFilterResult.DELETE` for exits. Source deletes must be kept
when the deleted document belonged to the set; absence of a post-image alone is not grounds for exclusion.

`filterMongoReplicationStream` takes a `MongoReplicationFilterOptions` object with `batches` and `filter` properties.
The exported `MongoReplicationFilter` callback returns a `MongoReplicationFilterResult` enum value. These types
and the shared batch definitions live in [MongoReplicationStreamTypes.ts](../../modules/module-mongodb/src/replication/MongoReplicationStreamTypes.ts) and are exported by the replication module.
The synchronous callback receives complete data changes, with document bodies still encoded as BSON buffers.
Return `MongoReplicationFilterResult.KEEP` to forward the event, `.EXCLUDE` to omit it, or `.DELETE` when a document
leaves the filtered set.
The helper retains source identity, token and transaction metadata on synthetic deletes. Checkpoint and DDL
events bypass the callback and remain intact. Membership evaluation must agree with snapshot selection.

The helper owns lazy iteration, batch boundaries, safe token forwarding, exclusion counts and iterator cleanup.
It preserves each change's `hasBufferedChanges`, including when trailing events are excluded. It counts exclusions
per batch even when no token is safe; applied filtering reports zero when nothing was excluded. Composed helpers
include upstream exclusion counts. Unfiltered streams retain `filteredCount: null`.

`ChangeStream` aggregates counts until it persists progress. Filtering events out in the MongoDB pipeline would
prevent the helper from counting them; use pipeline stages to annotate events and decide membership in the callback.
Keep callback state local to each `openChangeStream` invocation, since snapshot probes also open independent readers.

The lower-level `createMongoReplicationStreamBatch` remains available for specialized wrappers. Such wrappers must
preserve source batch boundaries, consume events fully, forward safe progress, and close iterators on failure or
cancellation. Ordinary filtering should use the shared helper.

## Durable progress and recovery

A raw batch token is a transport position, not automatically safe progress. The shared parser detects and joins
split fragments across batches, normalizes namespaces and skips events covered by legacy timestamp positions.

For example, raw batches `[A, B fragment 1]` and `[B fragment 2, C]` produce event iterables yielding A, then B and C.
After consuming the first iterable, `getResumeToken()` returns undefined because B is unfinished. After consuming
the second, it returns safe progress. B remains buffered until its final fragment. An earlier batch's undefined
result stays unchanged even after later fragments finish. A source ending with an incomplete event fails.

Empty batches and fully filtered batches must still have their event iterables exhausted. They can then carry
safe progress without yielding any events. While split assembly is incomplete, even empty batches return no
progress: saving their tokens would skip fragments held only in memory after a restart.

`bytesReplicatedMetric` counts all received bytes, including events the provider later
filters out. `chunksReplicatedMetric` counts all received batches, including batches where the provider filters out
every event. Both metrics are recorded before provider filtering.

The replication loop flushes preceding retained writes before saving a progress token. Excluded events count as
source activity, so filtered-only progress can be saved even while a checkpoint barrier is pending. A flush or
adapter failure must leave recovery at the last successfully persisted boundary. Empty, idle responses retain
the existing keepalive throttle.

Progress advances the recovery position; it does not itself publish a client checkpoint. Existing transaction,
snapshot and checkpoint barriers still control what clients can see. On restart, an exact resume token takes
precedence over timestamp deduplication: later operations in the same transaction can share its timestamp.

Returning progress regularly can reduce replay and long waits for matching events. It does not eliminate every
MongoDB timeout. Persistence currently follows safe active boundaries; changes to that cadence need separate
measurement and review.

## Local verification and benchmarking

The [filtering lifecycle test](../../modules/module-mongodb/test/src/filtering_lifecycle.test.ts) uses real MongoDB
pre/post-images and bucket storage to check snapshot selection, membership entries and exits, source deletes,
and restart. The [provider diagnostic test](../../modules/module-mongodb/test/src/query_provider_diagnostics.test.ts)
reopens storage to verify that removing a required provider leaves a persisted replication error.

The [throughput experiment](../../modules/module-mongodb/benchmarks/replication_branch_throughput.benchmark.ts)
lives in `benchmarks/`, separate from normal and `SLOW_TESTS` runs. From `modules/module-mongodb`,
run `TEST_POSTGRES_STORAGE=false pnpm benchmark:replication -t 'storage v2'`. The dedicated configuration enables
GC and serial fork execution. Run again with `MEMORY_EAGER=true` to compare parsing modes. It reports measurements,
not a performance pass/fail threshold, and defaults to about 2 GiB of source payloads. For a small smoke run, use
`THROUGHPUT_ROWS=20 THROUGHPUT_PAYLOAD_BYTES=1024`. Run benchmarks sequentially against idle test databases.

## Tests in external modules

The package exports `test_utils.ChangeStreamTestContext` from `src/test-utils`. Callers provide a storage factory,
normalized source connection options and metrics. The helper has no dependency on this suite's environment or
storage implementation.

```ts
await using context = await test_utils.ChangeStreamTestContext.open({
  factory,
  connectionOptions,
  metrics,
  storageVersion: 4,
  streamOptions: { createReplicationQueryProvider }
});
```

Opening clears the selected source database and storage by default. Reopen with `doNotClear: true` and
`loadActiveSyncRules()` or `loadNextSyncRules()` to test recovery against the same databases. `stop()` aborts and
drains replication before inspecting durable state. Subclasses can reuse resource setup and failure cleanup with
the protected `openWith({ options, createContext })` helper.

`updateSyncRules(yaml)` parses sync configs with the shared `SqlSyncRules.fromYaml()` parser, which accepts
`config.source_table_options` and MongoDB pre-filtering expressions. External modules supply a query provider
for expression compilation and execution; they do not register additional sync config parsers.

## Provider availability

A saved sync config with MongoDB pre-filtering expressions requires a registered query provider factory. Replication reports an error before snapshot setup if that factory is absent, including after a downgrade. Restore the required module, or remove the filters and deploy a new sync config to start replacement processing. Configured wildcard expressions still require support when individual collections disable their filters, since new collections may match later.
