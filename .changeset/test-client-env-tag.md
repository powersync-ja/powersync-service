---
'@powersync/service-core': patch
'test-client': patch
---

Resolve `!env` tags when the test client parses `powersync.yaml`, and add an optional `--env <path>` flag to load a `.env` file first.
