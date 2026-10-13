# MongoDB replication adapters

`ChangeStreamReplicator` accepts a `createReplicationQueryProvider` factory. It creates a provider for each
replication attempt, passing the hydrated sync config, connection tag and default database. The same provider
serves the attempt's snapshots, streaming reader, snapshot barriers and resume validation.

The default provider preserves ordinary MongoDB and DocumentDB replication:

```ts
const provider: MongoReplicationQueryProvider = {
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
  const filteredCount = batch.metadata.filteredCount;
  // Aggregate non-null counts, including batches without a safe token.
  const resumeToken = batch.getResumeToken();
  if (resumeToken != null) {
    await persistResumeToken(resumeToken);
  }
}
```

Consume events once and completely before accessing the token, metadata or the next batch. Both token and
metadata access throw before exhaustion, including after the last event but before the iterator reports completion.
After exhaustion, `getResumeToken()` returns a stable safe token or `undefined`; `metadata` is a cached immutable
object. If processing fails or is cancelled, close the iterators without saving a token.

Filtering providers wrap `batch.events` with a synchronous generator. Use
`createMongoReplicationStreamBatch({ events, resolveResumeToken, resolveMetadata })` to finalize both results
after successful exhaustion. Forward the source batch's safe token and report excluded complete events in
`metadata.filteredCount` for this batch only, including when no token is safe. Use `null` when no filtering was
applied, `0` when filtering excluded nothing, or a positive count. The metadata callback defaults to `null`.

`ChangeStream` aggregates non-null counts across batches until it persists and reports progress, then resets
its aggregate to `null`. Unfiltered traffic omits filtering details from logs; applied filtering can report zero.
Later parsing or filtering cannot change an earlier batch's cached token or metadata.

Providers may discard marked events or translate exits from a filtered set into deletes. Filtering events out in
the MongoDB pipeline prevents the provider from recording their progress. Preserve checkpoint and DDL events,
and retain the original key, namespace, token and transaction metadata on synthetic deletes.

Each change's `hasBufferedChanges` records whether source entries followed that event, including entries a filter
may exclude. Preserve it when forwarding or transforming the change; the consumer cannot infer this from the
length of a lazy iterable. This flag controls checkpoint batching, not safe progress.

Wrappers must preserve order and close the underlying iterators on completion, failure or cancellation.
Use `for await` for source batches and `for...of` for their events so errors and early returns trigger cleanup.

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
