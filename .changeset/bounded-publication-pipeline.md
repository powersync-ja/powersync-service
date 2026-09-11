---
'@powersync/service-module-mongodb-storage': patch
'@powersync/service-module-mongodb': patch
'@powersync/service-core': patch
---

Pipeline MongoDB v3/v4 replication with bounded publication groups, overlapping uploads, ordered publication, and queued page resume positions. Reuse uploads on transaction retries and protect abandoned uploads with orphan markers. Use existing operation ID reservations and per-stream fencing without a global publication lock.

Share the publication queue and pending row state between snapshot and streaming writers of the same stream. Use the common publication machinery for v1/v2 while retaining eager publication and legacy storage formats.
