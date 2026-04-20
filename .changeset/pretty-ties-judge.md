---
'@powersync/service-sync-rules': patch
---

Support multiple references between the same tables as join conditions for Sync Streams (the compiler would previously crash due to an "internal circular reference error").
