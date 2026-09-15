---
'@powersync/service-core': patch
---

Fix loading storage migrations on Windows: migration scripts are now imported via file URLs, since the Node.js ESM loader rejects absolute Windows paths such as `C:\...`.
