---
'@powersync/service-module-mysql': patch
---

Enable TCP keepalive on MySQL connections, as well as a periodic liveness probe on the control connection, so that idle connections are no longer silently dropped by stateful firewalls.
