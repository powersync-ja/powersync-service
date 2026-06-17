# Azure Cosmos DB for MongoDB vCore — Limitations

PowerSync can replicate from Azure Cosmos DB for MongoDB vCore (and the underlying DocumentDB engine), but Cosmos is not fully MongoDB-compatible. This document lists the limitations that **affect users** — behaviour that differs from a standard MongoDB source in a way a deployment can observe.

Internal implementation details (how checkpoints and LSNs work on Cosmos) are documented separately in [cosmos-db-lsn-sentinel-checkpoints.md](./cosmos-db-lsn-sentinel-checkpoints.md).

> **Cosmos DB support is experimental.** Validate it against your own workload before relying on it in production.

## Post-images are not supported

Cosmos DB does not support `changeStreamPreAndPostImages`. The connection must be configured with `post_images: off` (the default). `read_only` and `auto_configure` will fail.

This does not reduce correctness: Cosmos always includes the full current document on update events, so updates and deletes still replicate correctly. It only means the post-image _configuration modes_ cannot be used.

## Collection drop and rename are not replicated

Dropping or renaming a replicated collection on the source is **not** propagated to PowerSync on Cosmos:

- After a collection is dropped, its already-synced rows remain in PowerSync storage instead of being removed.
- After a collection is renamed, its rows remain under the old name and the new name is not picked up automatically.

These DDL events are delivered differently (or not at all) through Cosmos's cluster-level change stream. To recover, redeploy sync rules / trigger a resync. Regular document inserts, updates, and deletes are unaffected.

## Large initial snapshots may not complete

Cosmos DB retains only a limited amount of change-feed history (on the order of a few hundred MB). Initial replication takes a snapshot and then resumes streaming from a position captured before the snapshot. For a large or busy source, the snapshot can take long enough that this position ages out of the retention window before streaming resumes — replication then restarts from scratch and can loop.

In practice, Cosmos initial replication is currently suited to datasets small enough to snapshot well within the change-feed retention window. (The planned fix is incremental reprocessing, which consumes the change stream from the moment the snapshot begins.)

## Rows of 15 MiB or larger are not synced

PowerSync does not sync a single row whose serialized size is 15 MiB or larger; such rows are dropped with a logged error. Cosmos DB itself permits documents up to 16 MiB, so a document that is valid on the source can exceed PowerSync's row-size limit and silently not appear for clients. This limit is not Cosmos-specific, but it is more reachable on Cosmos because the change event always carries the full document.

## Large documents replicate very slowly

Cosmos DB delivers large change events much more slowly than standard MongoDB. In testing, a single change event carrying a ~14 MiB document took roughly **18 seconds** to fetch from the change stream (~0.8 MB/s). The document still replicates correctly — it is not dropped — but:

- Replication latency spikes sharply around large documents, and the spike applies to **updates** too: `updateLookup` re-fetches the full current document into the event, so updating a tiny field on a large document still incurs the full large-event cost.
- A single large event blocks the change stream while it is being fetched, delaying every change behind it (including checkpoint advancement). Consistent checkpoints — and therefore write-checkpoint resolution — can lag by the fetch time.
- For documents close to the 16 MiB limit, the fetch time may approach the change stream's socket/`maxTimeMS` budget, which would surface as a stream timeout and restart.

If your workload includes large documents (especially frequently-updated ones), validate replication latency against your cluster.

## Changing the source database is not detected

On standard MongoDB, repointing a connection at a different source database invalidates the stored replication position and forces a resync. On Cosmos the change stream is cluster-scoped, so the stored position stays valid when only the database name changes — replication silently continues against the new (typically empty) database instead of failing.

If you change the source database for an existing connection, trigger a resync manually.

## Write checkpoints may resolve slightly early

On standard MongoDB, write-checkpoint resolution is ordered by `clusterTime`, which guarantees the caller's write is replicated before the checkpoint resolves. Cosmos has no usable `clusterTime`, so the implementation uses a sentinel write (the `_standalone_checkpoint` counter) as the replication head and resolves the write checkpoint once the change stream commits at or beyond that sentinel.

Cosmos only guarantees change ordering _per document_, not across documents. The sentinel write lives in a different document from the caller's data write, so the sentinel's change event can be delivered ahead of the data write's event. In that window a write checkpoint can resolve a moment before the corresponding data is actually visible to clients — a client may briefly not see its own just-written data even after the write checkpoint is acknowledged.

This is inherent to Cosmos's ordering guarantees and is part of why Cosmos support is experimental. If read-your-writes consistency is critical for your workload, validate it against your cluster.

## Throughput and latency

Cosmos DB's change stream historically delivered events more slowly than standard MongoDB (one operation per poll, with idle polls returning immediately). Microsoft has since added long-polling (`maxAwaitTime`) and event batching. Throughput and latency for high-write workloads should be validated against your target cluster, as these capabilities depend on the Cosmos DB version in use.

## Operational note: the internal checkpoints collection

PowerSync maintains a `_powersync_checkpoints` collection in the source database for replication bookkeeping. Do not drop it or delete its documents: doing so disrupts replication (dropping the collection forces a resync; the implementation defends against the standalone counter document being deleted, but deleting it should still be avoided).
