# @powersync/sync-plan-evaluator-rs

Rust evaluator for serialized sync plans.

Current scope:
- Load serialized sync plans (`version: unstable`).
- Evaluate bucket data rows.
- Evaluate parameter index rows.
- Prepare and resolve bucket parameter queries (static + dynamic lookups).

This package is intentionally standalone and does not modify existing service code.
