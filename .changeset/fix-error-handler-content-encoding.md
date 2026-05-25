---
'@powersync/service-core': patch
---

Add a custom Fastify error handler so uncaught errors honour the negotiated `Content-Encoding` (gzip/zstd) instead of returning a header that doesn't match the body.
