---
'@powersync/service-core': patch
'powersync-open-service': patch
---

- Use a LRU cache for checksum computations, improving performance and reducing MongoDB database load.
- Return zero checksums to the client instead of omitting, to help with debugging sync issues.
