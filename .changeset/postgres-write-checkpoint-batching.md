---
'@powersync/service-module-postgres-storage': patch
---

Combine the write checkpoint lookups of concurrent connections in Postgres storage. All connections receive a new checkpoint at about the same time; instead of one `write_checkpoints` query per connection and checkpoint, the lookups for the same LSN are now made in a single query.
