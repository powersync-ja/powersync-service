---
'@powersync/service-module-mongodb-storage': patch
---

Drop unused MongoDB storage v1/v2 collections instead of deleting their records individually when no processing, active, or errored v1/v2 replication streams need them. Initialize their indexes only when a v1/v2 stream starts, so new v4 instances do not create these collections.
