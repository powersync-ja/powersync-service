---
'@powersync/service-image': minor
---

- Added type casting for YAML tag functions. YAML config can now substitute environment variables as boolean and number types.

```yaml
# PowerSync config
api:
  parameters:
    max_buckets_per_connection: !env_number PS_MAX_BUCKETS
healthcheck:
  probes:
    http: !env_boolean PS_MONGO_HEALTHCHECK
```

- Added the ability to customize healthcheck probe exposure in the configuration. Backwards compatibility is maintained if no `healthcheck->probes` config is provided.

```yaml
healthcheck:
  probes:
    # Health status can be accessed by reading files (previously always enabled)
    filesystem: true
    # Health status can be accessed via HTTP requests (previously enabled for API and UNIFIED service modes)
    http: true
```
