---
'@powersync/service-image': minor
---

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

- Added the ability to customize healthcheck probe exposure in the configuration. Backwards compatibility is maintained if no `healthcheck->probes` config is provided.

```yaml
healthcheck:
  probes:
    # Health status can be accessed by reading files (previously always enabled)
    use_filesystem: true
    # Health status can be accessed via HTTP requests (previously enabled for API and UNIFIED service modes)
    use_http: true
```
