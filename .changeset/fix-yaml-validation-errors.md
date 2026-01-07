---
'@powersync/service-core': patch
---

Fix swallowed YAML validation errors in config parsing. When both YAML and JSON parsing fail, the error message now includes the detailed YAML validation error (e.g., invalid !env substitutions) instead of only showing a generic JSON parsing error.
