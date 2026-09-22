---
'@powersync/service-core': patch
---

Merge overlapping static and dynamic bucket selections in service-core, preserving subscription metadata and priority for all storage backends. This prevents duplicate bucket operations and client checksum failures with Postgres storage.
