---
'@powersync/service-sync-rules': patch
---

Make `length()` count Unicode code points (matching SQLite) rather than JavaScript UTF-16 code units. Previously a string containing a non-BMP code point (emoji like `😀`, CJK Extension B–G, ancient scripts, etc.) had its length silently doubled on the server, producing a different bucket key than the SQLite client computes for the same expression.
