---
'@powersync/service-module-postgres': patch
'@powersync/service-sync-rules': patch
'@powersync/service-image': patch
---

Add the `custom_postgres_types` compatibility option. When enabled, domain, composite, enum, range, multirange and custom array types will get synced in a JSON representation instead of the raw postgres wire format.
