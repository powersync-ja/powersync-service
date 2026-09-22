---
'@powersync/service-core': patch
---

Fix duplicate bucket operations and client checksum failures with Postgres storage when a Sync Stream selects the same bucket through both static and dynamic parameters.
