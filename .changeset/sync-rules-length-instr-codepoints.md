---
'@powersync/service-sync-rules': patch
---

Fix `length()` and `instr()` to count characters (Unicode code points) rather than UTF-16 code units, matching SQLite. Previously `length('😀')` returned `2` instead of `1`, and `instr()` reported positions shifted by any non-BMP character before the match. This caused sync-rule expressions over text containing emoji or other non-BMP characters to diverge from the source database (the same class of issue as the earlier `substring()` code-point fix).
