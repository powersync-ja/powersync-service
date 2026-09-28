---
'@powersync/service-module-mongodb': patch
---

Report a MongoDB view matched by a wildcard table pattern as a warning, instead of failing sync config validation. Previously, a view matching a pattern such as `"orders%"` made the whole validation request fail with `Cannot get name for wildcard table`.
