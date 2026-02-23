---
'@powersync/service-core': patch
---

Add detailed logging for parameter query results to improve debugging when limits are exceeded. Checkpoint logs now include the total number of parameter query results (before deduplication), and error messages show a breakdown of the top 10 sync stream definitions contributing to the count.
