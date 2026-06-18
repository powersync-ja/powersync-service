# Technical Documentation

This folder contains technical documentation regarding the implementation of PowerSync.

For documentation on using PowerSync, see [docs.powersync.com](https://docs.powersync.com/).

## Specs

- [Replication protocol specs](./spec/replication/README.md): internal source replication, storage writer, checkpoint, and source-module contracts.

## Existing Notes

- [Sync protocol](./sync-protocol.md): client-facing sync stream messages.
- [Storage v3](./storage-v3.md): storage version 3 data structure notes.
- [Bucket properties](./bucket-properties.md): formal bucket operation properties.
- [Parameter lookups](./parameters-lookups.md): dynamic parameter lookup implementation.
- [Resolve tables flow](./resolve-tables-flow.md): source table discovery and snapshot lifecycle.
