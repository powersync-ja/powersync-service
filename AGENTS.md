# Agent Guidance for PowerSync Service

This repository contains the PowerSync service packages, source replication modules, storage modules, and technical documentation.

Keep this file concise. Use it to route agents to the right docs and nearby implementation examples rather than duplicating full specifications here.

## General Workflow

- Prefer existing module patterns over new abstractions.
- Keep changes scoped to the package or module relevant to the task.
- For integration tests, prefer the existing test utilities and fixtures in the relevant module.
- Prefer real implementations for tests that validate cross-module behavior. If a test only needs to observe calls or arguments, prefer Vitest spies such as `vi.spyOn()` before introducing mocks. Use mocks only when testing specific unit behavior or when a real dependency is impractical.

## Development Commands

- This workspace uses Node from [`.nvmrc`](.nvmrc) and pnpm from the root [`packageManager`](package.json). If the shell does not automatically load nvm or Corepack, run Node/pnpm commands through `source ~/.nvm/nvm.sh && nvm use && corepack pnpm ...`.
- Install dependencies with `corepack pnpm install`. CI enables Corepack before installing dependencies; match that locally when possible.
- This is a pnpm workspace. Use `corepack pnpm --filter <package-or-path> <script>` for focused package commands.
- Confirm package names in each package's `package.json`; do not use the top-level package name for package-specific filters. Path filters such as `--filter './modules/module-postgres'` are often the clearest option.
- Build the full workspace with `corepack pnpm build`. For a focused package, use `corepack pnpm --filter './modules/module-postgres' build`.
- Run core tests with `corepack pnpm test:core`. Run package tests with `corepack pnpm --filter './modules/module-postgres' test` or from the package directory with `corepack pnpm test`.
- Focus Vitest runs with a file or test name where useful, for example `corepack pnpm --filter './modules/module-postgres' test test/src/wal_stream.test.ts --run -t "record too large"`.
- Run `corepack pnpm format:dirty` before finishing changes that touch formatted source files. Use `corepack pnpm format:check` and `corepack pnpm validate:tsconfig-references` when formatting or project references may be affected.
- Local service setup is documented in [`DEVELOP.md`](DEVELOP.md). The service usually needs Postgres and MongoDB test services plus local `service/powersync.yaml` and `service/sync-rules.yaml` files.
- CI plans live in [`.github/workflows`](.github/workflows). Check them when choosing the closest local verification command for a change.

## Replication Work

Use this section when working on source replication modules, replication streams, source connector contracts, bucket storage writers, checkpoints, write checkpoints, initial snapshots, sync config deployment, or replication tests.

### Load Spec Context Selectively

Do not load every replication spec page by default. Start with the entry point, then load only the pages that match the task.

- Always start with [`docs/spec/replication/README.md`](docs/spec/replication/README.md) for the outside-in map and code anchors.
- Use [`01-core-concepts.md`](docs/spec/replication/01-core-concepts.md) when terminology is unclear, especially replication streams, modules, source positions, `SourceTable`, checkpoints, and write checkpoints.
- Use [`02-replication-lifecycle.md`](docs/spec/replication/02-replication-lifecycle.md) for `ReplicationEngine`, `AbstractReplicator`, job lifecycle, locks, cleanup, and stream states.
- Use [`03-source-connector-contract.md`](docs/spec/replication/03-source-connector-contract.md) for source config, `RouteAPI`, schema metadata, source stream requirements, keepalive responsibilities, and table discovery.
- Use [`04-storage-writer-contract.md`](docs/spec/replication/04-storage-writer-contract.md) for `SyncRulesBucketStorage`, `BucketStorageBatch`, `resolveTables()`, `save()`, `flush()`, `commit()`, `keepalive()`, `setResumeLsn()`, and snapshot state.
- Use [`05-snapshots-and-streaming.md`](docs/spec/replication/05-snapshots-and-streaming.md) for initial replication, resumable snapshots, source consistency boundaries, ongoing streaming, schema changes, and history loss.
- Use [`06-checkpoints.md`](docs/spec/replication/06-checkpoints.md) for storage checkpoints, bucket checksums, parameter lookups, managed write checkpoints, custom write checkpoints, and source-side marker tables.
- Use [`07-source-modules.md`](docs/spec/replication/07-source-modules.md) to compare existing Postgres, MongoDB, MySQL, SQL Server, and Convex implementations.
- Use [`08-ai-agent-plans.md`](docs/spec/replication/08-ai-agent-plans.md) when creating a new replication module or planning a substantial source-module change.

### Branch By Task

- New replication module: read [`README.md`](docs/spec/replication/README.md), [`01-core-concepts.md`](docs/spec/replication/01-core-concepts.md), [`03-source-connector-contract.md`](docs/spec/replication/03-source-connector-contract.md), [`07-source-modules.md`](docs/spec/replication/07-source-modules.md), and [`08-ai-agent-plans.md`](docs/spec/replication/08-ai-agent-plans.md). Start with Plan 1 in [`08-ai-agent-plans.md`](docs/spec/replication/08-ai-agent-plans.md), then load the later plan sections only as the implementation reaches them.
- Existing replication module change: read [`README.md`](docs/spec/replication/README.md), the relevant source section in [`07-source-modules.md`](docs/spec/replication/07-source-modules.md), and only the contract page that matches the change. For example, use [`03-source-connector-contract.md`](docs/spec/replication/03-source-connector-contract.md) for route/config/schema work, [`05-snapshots-and-streaming.md`](docs/spec/replication/05-snapshots-and-streaming.md) for snapshot or streaming behavior, and [`06-checkpoints.md`](docs/spec/replication/06-checkpoints.md) for write checkpoints.
- Storage writer or bucket storage change: read [`README.md`](docs/spec/replication/README.md), [`04-storage-writer-contract.md`](docs/spec/replication/04-storage-writer-contract.md), and [`06-checkpoints.md`](docs/spec/replication/06-checkpoints.md). Add [`05-snapshots-and-streaming.md`](docs/spec/replication/05-snapshots-and-streaming.md) only if snapshot consistency, history loss, or streaming boundaries are involved.
- Checkpoint or write-checkpoint change: read [`README.md`](docs/spec/replication/README.md) and [`06-checkpoints.md`](docs/spec/replication/06-checkpoints.md). Add [`03-source-connector-contract.md`](docs/spec/replication/03-source-connector-contract.md) when `RouteAPI.createReplicationHead()` or source-side marker behavior is involved.
- Replication lifecycle, locks, cleanup, or job orchestration: read [`README.md`](docs/spec/replication/README.md) and [`02-replication-lifecycle.md`](docs/spec/replication/02-replication-lifecycle.md). Add [`07-source-modules.md`](docs/spec/replication/07-source-modules.md) when comparing current module behavior.
- Tests only: load the spec page for the behavior being tested, plus the testing sections in [`08-ai-agent-plans.md`](docs/spec/replication/08-ai-agent-plans.md) when creating or changing a source-module test context.

After loading the relevant spec context, inspect the closest existing implementation and tests before editing.

### Replication Testing

- Prefer integration tests with real bucket storage implementations for snapshot, streaming, checkpoint, and write-checkpoint behavior.
- Import real test storage factories from storage modules, such as `@powersync/service-module-mongodb-storage` and `@powersync/service-module-postgres-storage`, and use `describeWithStorage`-style coverage where practical.
- Prefer Vitest spies such as `vi.spyOn()` when the test only needs to observe calls or arguments.
- Use mocks only for focused unit behavior or when a real dependency is impractical.
- Add a stream test context for new modules, following existing examples such as `WalStreamTestContext`, `ChangeStreamTestContext`, `BinlogStreamTestContext`, `CDCStreamTestContext`, or `ConvexStreamTestContext`.
