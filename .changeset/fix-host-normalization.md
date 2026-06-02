---
'@powersync/lib-services-framework': patch
'@powersync/lib-service-mongodb': patch
'@powersync/service-module-convex': patch
'@powersync/service-core': patch
---

Normalize socket addresses to bare hostnames before IP-range validation, so direct-IP literals with any port form are recognized as IPs.
