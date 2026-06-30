# Technical Documentation

This folder contains technical documentation regarding the implementation of PowerSync.

For documentation on using PowerSync, see [docs.powersync.com](https://docs.powersync.com/).

These docs are a work in progress. When changing behavior covered by a doc page, update the relevant section in the same change so the docs stay useful as implementation notes rather than historical notes.

## Replication

- [Replication protocol specs](./replication/README.md): internal source replication, storage writer, checkpoint, and source-module behavior.

## Specs

- [Sync protocol](./specs/sync-protocol.md): client-facing sync stream messages.

## Storage

- [Storage docs](./storage/README.md): bucket storage data structures, bucket invariants, compaction, and parameter lookups.

## Module Notes

- [Module docs](./modules/README.md): module-specific design notes for source connectors.
