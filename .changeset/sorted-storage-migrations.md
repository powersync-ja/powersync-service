---
'@powersync/service-core': patch
---

Run internal storage migrations in the order of their names, instead of the order `fs.readdir` returns them in.
