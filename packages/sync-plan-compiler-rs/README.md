# @powersync/sync-plan-compiler-rs

Rust compiler that converts sync streams YAML into serialized sync plan JSON.

Current scope:
- `config` + `streams` parsing from YAML-like source text.
- SQL subset support for `SELECT ... FROM ... [WHERE ...]`.
- Request-aware partition extraction for sync stream queriers.

Notes:
- This intentionally targets the new sync-plan path (`sync_config_compiler: true`).
- Legacy `bucket_definitions` parsing is out of scope.
