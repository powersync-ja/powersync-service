# Azure DocumentDB — Limitations

PowerSync can replicate from Azure DocumentDB (formerly Azure Cosmos DB for MongoDB vCore), but it is not fully MongoDB-compatible. This document lists the limitations that **affect users** — behaviour that differs from a standard MongoDB source in a way a deployment can observe.

Internal implementation details (how checkpoints and LSNs work on DocumentDB) are documented separately in [documentdb-lsn-sentinel-checkpoints.md](./documentdb-lsn-sentinel-checkpoints.md).

> **DocumentDB support is experimental.** APIs and behavior may change. We also can't yet guarantee continued support or long-term stability. This release is intended for early testing and to invite feedback. Your feedback will directly influence whether, and how, this integration evolve.

## Which Azure offering this supports

"Cosmos DB" spans more than one product, and they are not interchangeable for replication. The naming, briefly:

- **Azure Cosmos DB for MongoDB** is a MongoDB-compatible API offered in two deployment models:
  - **RU-based**: the original multitenant model, billed by throughput in [request units (RUs)](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/find-request-unit-charge). The [RU→DocumentDB migration announcement](https://devblogs.microsoft.com/cosmosdb/mongoru-to-documentdb/) names it _"RU-based Azure Cosmos DB for MongoDB"_.
  - **vCore-based**: dedicated instances with higher MongoDB compatibility.
- The **vCore** model was [renamed **Azure DocumentDB**](https://devblogs.microsoft.com/cosmosdb/azure-documentdb-is-now-generally-available/) (GA, November 2025), aligning it with the open-source [DocumentDB](https://documentdb.io) engine — now governed by the Linux Foundation — that powers it. Per the announcement, _"the former vCore-based Azure Cosmos DB for MongoDB offering is now Azure DocumentDB"_ and _"existing clusters automatically adopt the new name"_. So older clusters may still surface as "Cosmos DB for MongoDB vCore" and newer ones as "Azure DocumentDB", but they are the same offering and the same engine (see also the [Azure DocumentDB FAQ](https://learn.microsoft.com/en-us/azure/documentdb/faq)).

**PowerSync supports the vCore / Azure DocumentDB engine only — not the RU-based model.** The implementation detects it from the `hello` response (`internal.documentdb_versions`) and switches to the sentinel-checkpoint replication path described in the design notes. A source that does not report it is treated as standard MongoDB (Atlas / self-hosted / replica set), which is unaffected by anything in this document.

The **RU-based** model is a different engine with a different change-stream implementation ([RU change streams](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams) — for example, delete events and `operationType` are not exposed, and change order is documented only per shard key) and is **not supported**. If you are on the RU-based model, Microsoft provides a first-party [migration to Azure DocumentDB](https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/how-to-migrate-documentdb) ([GA](https://devblogs.microsoft.com/cosmosdb/mongoru-to-documentdb/)), which is also the path to making the source usable with PowerSync.

### The open-source DocumentDB engine is not supported

The [DocumentDB](https://documentdb.io) engine that powers Azure DocumentDB is also open-source (Linux Foundation), with a self-hostable `documentdb-local` Docker image. **PowerSync supports only the Azure-managed service, not the self-hosted open-source engine.** Change streams — which PowerSync replication fundamentally depends on — are a capability of the managed service, not the open-source engine: against `documentdb-local`, opening a change stream fails outright (`$changeStream is not supported yet in native pipeline`). The self-hosted image also does not report the `documentdb_versions` `hello` field used for detection, and presents as a sharded cluster (`msg: isdbgrid`), which is rejected. So the OSS engine cannot be used for replication, nor for running this connector's test suite.

Everything below describes the supported vCore / Azure DocumentDB engine.

## Post-images are not supported

DocumentDB does not support `changeStreamPreAndPostImages`. The connection must be configured with `post_images: off` (the default). `read_only` and `auto_configure` will fail.

This does not reduce correctness: DocumentDB always includes the full current document on update events, so updates and deletes still replicate correctly. It only means the post-image _configuration modes_ cannot be used.

## Collection drop and rename are not replicated

Dropping or renaming a replicated collection on the source is **not** propagated to PowerSync on DocumentDB:

- After a collection is dropped, its already-synced rows remain in PowerSync storage instead of being removed.
- After a collection is renamed, its rows remain under the old name and the new name is not picked up automatically.

These DDL events are delivered differently (or not at all) through DocumentDB's cluster-level change stream. To recover, redeploy sync rules / trigger a resync. Regular document inserts, updates, and deletes are unaffected.

## Large initial snapshots may not complete on legacy storage

DocumentDB retains only a limited amount of change-feed history (on the order of a few hundred MB). On storage v1/v2, initial replication takes a snapshot and then resumes streaming from a position captured before the snapshot. For a large or busy source, the snapshot can take long enough that this position ages out of the retention window before streaming resumes — replication then restarts from scratch and can loop.

Storage v3+ addresses this by consuming the change stream while the initial snapshot is still running and persisting resume progress as batches are processed. For large or busy DocumentDB initial snapshots, use storage v3+ rather than legacy storage.

## Rows of 15 MiB or larger are not synced

PowerSync does not sync a single row whose serialized size is 15 MiB or larger; such rows are dropped with a logged error. DocumentDB itself permits documents up to 16 MiB, so a document that is valid on the source can exceed PowerSync's row-size limit and silently not appear for clients. This limit is not DocumentDB-specific, but it is more reachable on DocumentDB because the change event always carries the full document.

## Large documents replicate very slowly

DocumentDB delivers large change events much more slowly than standard MongoDB. In testing, a single change event carrying a ~14 MiB document took roughly **18 seconds** to fetch from the change stream (~0.8 MB/s). The document still replicates correctly — it is not dropped — but:

- Replication latency spikes sharply around large documents, and the spike applies to **updates** too: `updateLookup` re-fetches the full current document into the event, so updating a tiny field on a large document still incurs the full large-event cost.
- A single large event blocks the change stream while it is being fetched, delaying every change behind it (including checkpoint advancement). Consistent checkpoints — and therefore write-checkpoint resolution — can lag by the fetch time.
- For documents close to the 16 MiB limit, the fetch time may approach the change stream's socket/`maxTimeMS` budget, which would surface as a stream timeout and restart.

If your workload includes large documents (especially frequently-updated ones), validate replication latency against your cluster.

## Operational note: the internal checkpoints collection

PowerSync maintains a `_powersync_checkpoints` collection in the source database for replication bookkeeping. Do not drop it or delete its documents: doing so disrupts replication (dropping the collection forces a resync; the implementation defends against the sentinel counter document being deleted, but deleting it should still be avoided).
