---
'@powersync/service-core': minor
---

- Added `ServiceContextMode` to `ServiceContext`. This conveys the mode in which the PowerSync service was started in.
- `RouterEngine` is now always present on `ServiceContext`. The router will only configure actual servers, when started, if routes have been registered.
- Added typecasting to `!env` YAML custom tag function. YAML config environment variable substitution now supports casting string environment variables to `number` and `boolean` types.

```yaml
replication:
  connections: []

storage:
  type: mongodb

api:
  parameters:
    max_buckets_per_connection: !env PS_MAX_BUCKETS::number

healthcheck:
  probes:
    use_http: !env PS_MONGO_HEALTHCHECK::boolean
```
