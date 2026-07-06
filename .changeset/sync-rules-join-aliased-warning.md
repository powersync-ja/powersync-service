---
'@powersync/service-sync-rules': patch
---

Emit a warning for Sync Streams with joins where the primary table has an alias. Adding an alias syncs the table under the changed name, which may be unintentional for joins if aliases are only added to simplify filters.

Closes #565.
