---
'@powersync/service-sync-rules': patch
---

Make `substring()` index by Unicode code points (matching SQLite) rather than UTF-16 code units. Previously, slicing through a non-BMP code point (emoji, CJK Extension, ancient scripts) returned a broken unpaired surrogate or split the character in half — server output silently disagreed with SQLite client output.
