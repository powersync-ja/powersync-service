---
'@powersync/lib-service-postgres': minor
'@powersync/service-jpgwire': minor
---

Add support for connection parameters passed via PostgreSQL connection string.

Supported parameters:
- connect_timeout: Connection timeout in seconds
- keepalives: Enable/disable TCP keepalives (0 or 1)
- keepalives_idle: Idle time before first keepalive
- keepalives_interval: Interval between keepalives
- keepalives_count: Number of keepalives before giving up

Example:
```
postgresql://user:pass@host/db?connect_timeout=300&keepalives=1&keepalives_idle=60
```

Closes #370
