# Convex Module Docs

These notes describe Convex-specific replication behavior and operational constraints.

- [Snapshot consistency](./snapshot-consistency.md): pinned snapshot cursors and delta stream consistency.
- [Schema change handling](./schema-change-handling.md): why Convex does not use conventional runtime schema-change detection.
- [Checkpoint requests](./convex-write-checkpoints.md): marker collection rationale for checkpoint requests, previously called managed write checkpoints, and idle sources.
