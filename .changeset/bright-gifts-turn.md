---
'@powersync/service-core': minor
---

- Added `ServiceContextMode` to `ServiceContext`. This conveys the mode in which the PowerSync service was started in.
- `RouterEngine` is now always present on `ServiceContext`. The router will only configure actual servers, when started, if routes have been registered.
- Added `!env_boolean` and `!env_number` YAML tag functions to `CompoundConfigCollector`.
