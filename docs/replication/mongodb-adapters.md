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

This repo defines the adapter boundary. A module that implements filtering owns its config validation,
expression compilation and decisions about which documents belong in the filtered set.

## Snapshot selection

`getSnapshotFilter(table)` receives a resolved physical source table and returns an additional `find` predicate,
or `null` for ordinary snapshots. An aggregation expression can be supplied as `{ $expr: expression }`.
The snapshot query combines that predicate with its `_id` continuation using `$and`, so pagination does not
replace an existing `$expr` or `_id` condition. Filtered snapshots use simple collation; unfiltered snapshots
retain the existing collection-default behavior.

The provider can implement `validateSource({ connectionManager, isDocumentDb })` to reject unsupported source
capabilities before queries open. Both replication and the snapshotter call it, so implementations should permit
repeated validation. The default provider adds no capability requirements.

## Opening and adapting changes

`openChangeStream({ open })` receives a callback that opens the shared source reader. Its position, namespace
selection, post-image requirements, timeouts and cancellation are already bound. Providers can supply
`pipelineStages` and the `fullDocumentBeforeChange` image option. Additional stages run after namespace selection
and before MongoDB's final large-event split stage.

The callback returns parsed, reassembled changes and safe progress boundaries. A provider can wrap this iterator
to discard marked events or translate exits from a filtered set into delete events. This requires access to the
returned stream: pipeline stages alone cannot discard an event in the service while preserving its source progress.

The returned items have two forms:

| Item                                               | Meaning                                                                                                               |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `{ type: 'change', event, hasBufferedChanges }`    | A complete source event to process. Row bodies remain raw BSON buffers.                                               |
| `{ type: 'progress', resumeToken, filteredCount }` | A safe source position covering completed events; `filteredCount` counts excluded events since the previous boundary. |

Wrappers must preserve ordering and close the underlying iterator when they finish, fail or are cancelled.
Using `for await` around `open(...)` provides this cleanup. Checkpoint and DDL events must remain intact.
Synthetic deletes must retain the original document key, namespace, resume token and transaction metadata.
`hasBufferedChanges` describes the received batch, including envelopes the wrapper may later exclude; preserve
it when forwarding an event so checkpoint batching still accounts for that work.

## Durable progress and recovery

The reader withholds progress while a split event is incomplete. After reassembly, its final token can safely
cover the complete event. Transport metrics count all received batches and bytes before provider filtering.

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

`updateSyncRules(yaml)` uses the supplied storage factory's sync config parser, including registered additional
parsers. External modules can therefore use their real YAML configuration in integration tests.
