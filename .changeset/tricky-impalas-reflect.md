---
'@powersync/service-core': patch
---

On the `/sync/stream` endpoint, set `X-Accel-Buffering: no` to prevent nginx from buffering the long-running response.
