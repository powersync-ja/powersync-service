# PowerSync Service — Code Walkthrough

*2026-04-12T05:55:29Z by Showboat 0.6.1*
<!-- showboat-id: cb3010e5-b5f1-4cc5-a0e3-fe9de933f035 -->

This walkthrough traces the PowerSync Service codebase from process boot to a live sync stream. PowerSync is a sync engine for local-first apps: it replicates data from a server-side database (Postgres, MongoDB, MySQL, or SQL Server) into per-user SQLite buckets that mobile and web clients can consume in real time. The service is the server component that makes this possible.

The codebase is a TypeScript monorepo managed by pnpm. It is organized into four top-level groups:

- **`service/`** — the Docker-image entry point; boots the process
- **`packages/`** — shared libraries (core logic, sync-rules compiler, JWT auth, pgwire, JSON, RSocket router, types, errors)
- **`modules/`** — pluggable modules for replication sources (Postgres, MongoDB, MySQL, MSSQL) and storage backends (MongoDB, Postgres)
- **`libs/`** — lightweight utility libraries (services framework, Mongo helpers, Postgres helpers)

We will follow the code path a request takes from `node service/src/entry.ts` all the way to a client receiving a streamed sync checkpoint.

## 1. Monorepo Structure

Let's look at the top-level layout and the pnpm workspace configuration.

```bash
ls -1d packages/* modules/* libs/* service/ test-client/
```

```output
libs/lib-mongodb
libs/lib-postgres
libs/lib-services
modules/module-core
modules/module-mongodb
modules/module-mongodb-storage
modules/module-mssql
modules/module-mysql
modules/module-postgres
modules/module-postgres-storage
modules/test_config.ts
packages/jpgwire
packages/jsonbig
packages/rsocket-router
packages/schema
packages/service-client
packages/service-core
packages/service-core-tests
packages/service-errors
packages/sync-rules
packages/types
service/
test-client/
```

The pnpm workspace binds these together:

```bash
head -8 pnpm-workspace.yaml
```

```output
packages:
  - 'packages/*'
  - 'libs/*'
  - 'modules/*'
  - 'service'
  - 'test-client'
  # exclude packages that are inside test directories
  - '!**/test/**'
```

Key packages at a glance:

| Package | Purpose |
|---------|---------|
| `packages/service-core` | The brain: replication abstractions, sync stream logic, routing, auth, storage interfaces, config loading, CLI |
| `packages/sync-rules` | Compiles YAML sync-rule definitions into query plans that determine which rows go into which client buckets |
| `packages/jpgwire` | Custom fork of pgwire for Postgres wire-protocol communication |
| `packages/rsocket-router` | RSocket (reactive stream) transport for WebSocket-based sync |
| `packages/types` | Shared TypeScript type definitions and config schema |
| `modules/module-postgres` | Postgres replication via WAL (logical decoding) |
| `modules/module-mongodb` | MongoDB replication via Change Streams |
| `modules/module-mongodb-storage` | MongoDB as the bucket storage backend |
| `modules/module-postgres-storage` | Postgres as the bucket storage backend |
| `modules/module-core` | Core module that sets up Fastify, RSocket, health checks, metrics |

## 2. The Entry Point — `service/src/entry.ts`

Everything begins here. When the Docker container starts, Node runs `service/lib/entry.js start`. This file boots the entire service in about 35 lines:

```bash
cat -n service/src/entry.ts
```

```output
     1	import { container, ContainerImplementation } from '@powersync/lib-services-framework';
     2	import * as core from '@powersync/service-core';
     3	
     4	import { CoreModule } from '@powersync/service-module-core';
     5	import { startServer } from './runners/server.js';
     6	import { startStreamRunner } from './runners/stream-worker.js';
     7	import { startUnifiedRunner } from './runners/unified-runner.js';
     8	import { createSentryReporter } from './util/alerting.js';
     9	import { DYNAMIC_MODULES } from './util/modules.js';
    10	
    11	// Initialize framework components
    12	container.registerDefaults();
    13	container.register(ContainerImplementation.REPORTER, createSentryReporter());
    14	
    15	const moduleManager = new core.modules.ModuleManager();
    16	moduleManager.register([new CoreModule()]);
    17	moduleManager.registerDynamicModules(DYNAMIC_MODULES);
    18	// This is a bit of a hack. Commands such as the teardown command or even migrations might
    19	// want access to the ModuleManager in order to use modules
    20	container.register(core.ModuleManager, moduleManager);
    21	
    22	// This is nice to have to avoid passing it around
    23	container.register(core.utils.CompoundConfigCollector, new core.utils.CompoundConfigCollector());
    24	
    25	// Generate Commander CLI entry point program
    26	const { execute } = core.entry.generateEntryProgram({
    27	  [core.utils.ServiceRunner.API]: startServer,
    28	  [core.utils.ServiceRunner.SYNC]: startStreamRunner,
    29	  [core.utils.ServiceRunner.UNIFIED]: startUnifiedRunner
    30	});
    31	
    32	/**
    33	 * Starts the program
    34	 */
    35	execute();
```

The startup sequence does four things at module scope (before any async work):

1. **DI container bootstrap** (line 12-13): Registers framework defaults (logger, probes) and a Sentry error reporter.
2. **Module registration** (line 15-20): Creates a `ModuleManager`, eagerly registers the `CoreModule` (Fastify/RSocket/health checks), and records lazy loaders for database-specific modules.
3. **Config collector** (line 23): Registers a `CompoundConfigCollector` that can read config from YAML files, base64 env vars, or filesystem paths.
4. **CLI program** (line 26-30): Uses Commander.js to create a CLI with subcommands (`start`, `migrate`, `teardown`, `compact`, `test-connection`). The `start` command maps three runner types — API, SYNC, and UNIFIED — to their handler functions.

The default runner type is UNIFIED (both API server and replication worker in one process).

The CLI is generated in `packages/service-core/src/entry/cli-entry.ts`:

```bash
sed -n "17,44p" packages/service-core/src/entry/cli-entry.ts
```

```output
export function generateEntryProgram(startHandlers?: Record<utils.ServiceRunner, utils.Runner>) {
  const entryProgram = new Command();
  entryProgram.name('powersync-runner').description('CLI to initiate a PowerSync service runner');

  registerTearDownAction(entryProgram);
  registerMigrationAction(entryProgram);
  registerCompactAction(entryProgram);
  registerTestConnectionAction(entryProgram);

  if (startHandlers) {
    registerStartAction(entryProgram, startHandlers);
  }

  return {
    program: entryProgram,
    /**
     * Executes the main program. Ends the NodeJS process if an exception was caught.
     */
    execute: async function runProgram() {
      try {
        await entryProgram.parseAsync();
      } catch (e) {
        logger.error('Fatal startup error - exiting with code 150.', e);
        process.exit(150);
      }
    }
  };
}
```

## 3. The Module System — Dynamic Loading

PowerSync uses a plugin architecture. Modules are loaded dynamically based on the user's configuration. If your config says `storage.type: mongodb` and `connections[0].type: postgresql`, only the MongoDB storage module and Postgres replication module are loaded — MySQL and MSSQL code is never imported.

The module map is defined in `service/src/util/modules.ts`:

```bash
cat -n service/src/util/modules.ts
```

```output
     1	import * as core from '@powersync/service-core';
     2	
     3	export const DYNAMIC_MODULES: core.ModuleLoaders = {
     4	  connection: {
     5	    mongodb: () => import('@powersync/service-module-mongodb').then((module) => new module.MongoModule()),
     6	    mysql: () => import('@powersync/service-module-mysql').then((module) => new module.MySQLModule()),
     7	    mssql: () => import('@powersync/service-module-mssql').then((module) => new module.MSSQLModule()),
     8	    postgresql: () => import('@powersync/service-module-postgres').then((module) => new module.PostgresModule())
     9	  },
    10	  storage: {
    11	    mongodb: () =>
    12	      import('@powersync/service-module-mongodb-storage').then((module) => new module.MongoStorageModule()),
    13	    postgresql: () =>
    14	      import('@powersync/service-module-postgres-storage').then((module) => new module.PostgresStorageModule())
    15	  }
    16	};
```

Each entry is a lazy `() => import(...)` — a dynamic ESM import that only runs when the config requires that module. The loader in `packages/service-core/src/modules/loader.ts` wires this up:

```bash
cat -n packages/service-core/src/modules/loader.ts
```

```output
     1	import { ResolvedPowerSyncConfig } from '../util/util-index.js';
     2	import { AbstractModule } from './AbstractModule.js';
     3	
     4	interface DynamicModuleMap {
     5	  [key: string]: () => Promise<AbstractModule>;
     6	}
     7	
     8	export interface ModuleLoaders {
     9	  storage: DynamicModuleMap;
    10	  connection: DynamicModuleMap;
    11	}
    12	/**
    13	 * Utility function to dynamically load and instantiate modules.
    14	 */
    15	export async function loadModules(config: ResolvedPowerSyncConfig, loaders: ModuleLoaders) {
    16	  const requiredConnections = [...new Set(config.connections?.map((connection) => connection.type) || [])];
    17	  const missingConnectionModules: string[] = [];
    18	  const modulePromises: Promise<AbstractModule>[] = [];
    19	
    20	  // 1. Map connection types to their module loading promises making note of any
    21	  // missing connection types.
    22	  requiredConnections.forEach((connectionType) => {
    23	    const modulePromise = loaders.connection[connectionType];
    24	    if (modulePromise !== undefined) {
    25	      modulePromises.push(modulePromise());
    26	    } else {
    27	      missingConnectionModules.push(connectionType);
    28	    }
    29	  });
    30	
    31	  // Fail if any connection types are not found.
    32	  if (missingConnectionModules.length > 0) {
    33	    throw new Error(`Invalid connection types: "${[...missingConnectionModules].join(', ')}"`);
    34	  }
    35	
    36	  if (loaders.storage[config.storage.type] !== undefined) {
    37	    modulePromises.push(loaders.storage[config.storage.type]());
    38	  } else {
    39	    throw new Error(`Invalid storage type: "${config.storage.type}"`);
    40	  }
    41	
    42	  // 2. Dynamically import and instantiate module classes and resolve all promises
    43	  // raising errors if any modules could not be imported.
    44	  const moduleInstances = await Promise.all(modulePromises);
    45	
    46	  return moduleInstances;
    47	}
```

The logic is straightforward: read which connection types and storage type the config specifies, call the corresponding loaders, fail hard if any are missing. All modules implement the `AbstractModule` interface:

```bash
sed -n "17,37p" packages/service-core/src/modules/AbstractModule.ts
```

```output
export abstract class AbstractModule {
  protected logger: winston.Logger;

  protected constructor(protected options: AbstractModuleOptions) {
    this.logger = logger.child({ name: `Module:${options.name}` });
  }

  /**
   *  Initialize the module using any required services from the ServiceContext
   */
  public abstract initialize(context: ServiceContextContainer): Promise<void>;

  /**
   *  Permanently clean up and dispose of any configuration or state for this module.
   */
  public abstract teardown(options: TearDownOptions): Promise<void>;

  public get name() {
    return this.options.name;
  }
}
```

Every module gets one chance to `initialize()` — that's where it hooks into the ServiceContext by registering replicators, storage providers, route API adapters, health probes, and metrics.

## 4. The ServiceContext — Wiring It All Together

When the `start` command runs (say, in UNIFIED mode), the runner creates a `ServiceContextContainer`. This is the central nervous system of the process — it owns every engine and manages their lifecycles. Let's look at the unified runner first:

```bash
cat -n service/src/runners/unified-runner.ts
```

```output
     1	import { container, logger } from '@powersync/lib-services-framework';
     2	import * as core from '@powersync/service-core';
     3	
     4	import { logBooting } from '../util/version.js';
     5	import { registerReplicationServices } from './stream-worker.js';
     6	
     7	/**
     8	 * Starts an API server
     9	 */
    10	export const startUnifiedRunner = async (runnerConfig: core.utils.RunnerConfig) => {
    11	  logBooting('Unified Container');
    12	
    13	  const config = await core.utils.loadConfig(runnerConfig);
    14	
    15	  const moduleManager = container.getImplementation(core.modules.ModuleManager);
    16	
    17	  const serviceContext = new core.system.ServiceContextContainer({
    18	    serviceMode: core.system.ServiceContextMode.UNIFIED,
    19	    configuration: config
    20	  });
    21	  registerReplicationServices(serviceContext);
    22	
    23	  await moduleManager.initialize(serviceContext);
    24	
    25	  await core.migrations.ensureAutomaticMigrations({
    26	    serviceContext
    27	  });
    28	
    29	  logger.info('Starting service...');
    30	  await serviceContext.lifeCycleEngine.start();
    31	  logger.info('Service started');
    32	
    33	  await container.probes.ready();
    34	
    35	  // Enable in development to track memory usage:
    36	  // trackMemoryUsage();
    37	};
```

The boot sequence is:

1. **Load config** (line 13) — reads YAML / env vars into a `ResolvedPowerSyncConfig`
2. **Create ServiceContext** (line 17-20) — instantiates all engines
3. **Register replication** (line 21) — creates a `ReplicationEngine` and wires it into the lifecycle
4. **Initialize modules** (line 23) — each module's `initialize()` plugs into the context
5. **Run migrations** (line 25-27) — ensures the storage database schema is up-to-date
6. **Start lifecycle** (line 30) — fires `start()` on every registered engine in dependency order
7. **Signal readiness** (line 33) — marks the health probe as ready

Now let's see what the ServiceContext actually creates:

```bash
sed -n "13,24p" packages/service-core/src/system/ServiceContext.ts
```

```output
export interface ServiceContext {
  configuration: utils.ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  metricsEngine: metrics.MetricsEngine;
  replicationEngine: replication.ReplicationEngine | null;
  routerEngine: routes.RouterEngine;
  storageEngine: storage.StorageEngine;
  migrations: PowerSyncMigrationManager;
  syncContext: SyncContext;
  serviceMode: ServiceContextMode;
  eventsEngine: EventsEngine;
}
```

The constructor in `ServiceContextContainer` creates each engine and registers lifecycle hooks on the `LifeCycledSystem`. This system guarantees ordered startup and shutdown — the storage engine starts before the router, the router stops before storage, etc. Here are the lifecycle registrations:

```bash
sed -n "55,104p" packages/service-core/src/system/ServiceContext.ts
```

```output
  constructor(options: ServiceContextOptions) {
    this.serviceMode = options.serviceMode;
    const { configuration } = options;
    this.configuration = configuration;

    this.lifeCycleEngine = new LifeCycledSystem();

    this.storageEngine = new storage.StorageEngine({
      configuration
    });
    this.storageEngine.registerListener({
      storageFatalError: (error) => {
        // Propagate the error to the lifecycle engine
        this.lifeCycleEngine.stopWithError(error);
      }
    });

    this.eventsEngine = new EventsEngine();
    this.lifeCycleEngine.withLifecycle(this.eventsEngine, {
      stop: (emitterEngine) => emitterEngine.shutDown()
    });

    this.lifeCycleEngine.withLifecycle(this.storageEngine, {
      start: (storageEngine) => storageEngine.start(),
      stop: (storageEngine) => storageEngine.shutDown()
    });

    this.routerEngine = new routes.RouterEngine();
    this.lifeCycleEngine.withLifecycle(this.routerEngine, {
      stop: (routerEngine) => routerEngine.shutDown()
    });

    this.syncContext = new SyncContext({
      maxDataFetchConcurrency: configuration.api_parameters.max_data_fetch_concurrency,
      maxBuckets: configuration.api_parameters.max_buckets_per_connection,
      maxParameterQueryResults: configuration.api_parameters.max_parameter_query_results
    });

    const migrationManager = new MigrationManager();
    container.register(framework.ContainerImplementation.MIGRATION_MANAGER, migrationManager);

    this.lifeCycleEngine.withLifecycle(migrationManager, {
      // Migrations should be executed before the system starts
      start: () => migrationManager[Symbol.asyncDispose]()
    });

    this.lifeCycleEngine.withLifecycle(this.eventsEngine, {
      stop: (emitterEngine) => emitterEngine.shutDown()
    });
  }
```

Notice the error propagation pattern on line 66-68: if the storage engine hits a fatal error, it stops the entire lifecycle. And the `SyncContext` (line 88-92) carries concurrency limits — `maxDataFetchConcurrency` controls how many sync streams can fetch bucket data in parallel, preventing the service from being overwhelmed.

## 5. Configuration Loading

Config is loaded by `core.utils.loadConfig()`, which delegates to a `CompoundConfigCollector`. The collector supports multiple config sources — a YAML file on disk, a base64-encoded env var, or environment variable fallbacks:

```bash
cat -n packages/service-core/src/util/config.ts
```

```output
     1	import * as fs from 'fs/promises';
     2	import winston from 'winston';
     3	
     4	import { container, DEFAULT_LOG_FORMAT, DEFAULT_LOG_LEVEL, LogFormat, logger } from '@powersync/lib-services-framework';
     5	import { configFile } from '@powersync/service-types';
     6	import { ResolvedPowerSyncConfig, RunnerConfig } from './config/types.js';
     7	import { CompoundConfigCollector } from './util-index.js';
     8	
     9	export function configureLogger(config?: configFile.LoggingConfig): void {
    10	  const level = process.env.PS_LOG_LEVEL ?? config?.level ?? DEFAULT_LOG_LEVEL;
    11	  const format =
    12	    (process.env.PS_LOG_FORMAT as configFile.LoggingConfig['format']) ?? config?.format ?? DEFAULT_LOG_FORMAT;
    13	  const winstonFormat = format === 'json' ? LogFormat.production : LogFormat.development;
    14	
    15	  // We want the user to always be aware that a log level was configured (they might forget they set it in the config and wonder why they aren't seeing logs)
    16	  // We log this using the configured format, but before we configure the level.
    17	  logger.configure({ level: DEFAULT_LOG_LEVEL, format: winstonFormat, transports: [new winston.transports.Console()] });
    18	  logger.info(`Configured logger with level "${level}" and format "${format}"`);
    19	
    20	  logger.configure({ level, format: winstonFormat, transports: [new winston.transports.Console()] });
    21	}
    22	
    23	/**
    24	 * Loads the resolved config using the registered config collector
    25	 */
    26	export async function loadConfig(runnerConfig: RunnerConfig) {
    27	  const collector = container.getImplementation(CompoundConfigCollector);
    28	  const config = await collector.collectConfig(runnerConfig);
    29	  configureLogger(config.base_config.system?.logging);
    30	  return config;
    31	}
    32	
    33	export async function loadSyncRules(config: ResolvedPowerSyncConfig): Promise<string | undefined> {
    34	  const sync_rules = config.sync_rules;
    35	  if (sync_rules.content) {
    36	    return sync_rules.content;
    37	  } else if (sync_rules.path) {
    38	    return await fs.readFile(sync_rules.path, 'utf-8');
    39	  }
    40	}
```

The config collectors are pluggable. Let's see what sources are tried:

```bash
ls -1 packages/service-core/src/util/config/collectors/impl/
```

```output
base64-config-collector.ts
fallback-config-collector.ts
filesystem-config-collector.ts
yaml-env.ts
```

Three collectors are tried in order:

- **`base64-config-collector`** — reads from the `POWERSYNC_CONFIG_B64` env var (useful in Docker/K8s where mounting a file is inconvenient)
- **`filesystem-config-collector`** — reads a YAML file from disk (default: `powersync.yaml` or the path passed via `--config`)
- **`fallback-config-collector`** — constructs config from individual environment variables (`PS_DATABASE_URL`, `PS_STORAGE_TYPE`, etc.)

Similarly, sync rules can be inline YAML, a file path, or base64-encoded. The config is validated against a JSON Schema generated from ts-codec type definitions — type safety from config file to runtime.

## 6. The Storage Engine — Where Sync Data Lives

The `StorageEngine` is responsible for persisting replicated data into "buckets" that clients can consume. It uses a provider pattern — modules register a `StorageProvider`, and the engine activates the one matching the config:

```bash
cat -n packages/service-core/src/storage/StorageEngine.ts
```

```output
     1	import { BaseObserver, logger, ServiceError } from '@powersync/lib-services-framework';
     2	import { ResolvedPowerSyncConfig } from '../util/util-index.js';
     3	import { BucketStorageFactory } from './BucketStorageFactory.js';
     4	import { ActiveStorage, StorageProvider } from './StorageProvider.js';
     5	
     6	export type StorageEngineOptions = {
     7	  configuration: ResolvedPowerSyncConfig;
     8	};
     9	
    10	export interface StorageEngineListener {
    11	  storageActivated: (storage: BucketStorageFactory) => void;
    12	  storageFatalError: (error: ServiceError) => void;
    13	}
    14	
    15	export class StorageEngine extends BaseObserver<StorageEngineListener> {
    16	  // TODO: This will need to revisited when we actually support multiple storage providers.
    17	  private storageProviders: Map<string, StorageProvider> = new Map();
    18	  private currentActiveStorage: ActiveStorage | null = null;
    19	
    20	  constructor(private options: StorageEngineOptions) {
    21	    super();
    22	  }
    23	
    24	  get activeBucketStorage(): BucketStorageFactory {
    25	    return this.activeStorage.storage;
    26	  }
    27	
    28	  get activeStorage(): ActiveStorage {
    29	    if (!this.currentActiveStorage) {
    30	      throw new Error(`No storage provider has been initialized yet.`);
    31	    }
    32	
    33	    return this.currentActiveStorage;
    34	  }
    35	
    36	  /**
    37	   * Register a provider which generates a {@link BucketStorageFactory}
    38	   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
    39	   */
    40	  registerProvider(provider: StorageProvider) {
    41	    this.storageProviders.set(provider.type, provider);
    42	  }
    43	
    44	  public async start(): Promise<void> {
    45	    logger.info('Starting Storage Engine...');
    46	    const { configuration } = this.options;
    47	    this.currentActiveStorage = await this.storageProviders.get(configuration.storage.type)!.getStorage({
    48	      resolvedConfig: configuration
    49	    });
    50	    this.iterateListeners((cb) => cb.storageActivated?.(this.activeBucketStorage));
    51	    this.currentActiveStorage.onFatalError?.((error) => {
    52	      this.iterateListeners((cb) => cb.storageFatalError?.(error));
    53	    });
    54	    logger.info(`Successfully activated storage: ${configuration.storage.type}.`);
    55	    logger.info('Successfully started Storage Engine.');
    56	  }
    57	
    58	  /**
    59	   *  Shutdown the storage engine, safely shutting down any activated storage providers.
    60	   */
    61	  public async shutDown(): Promise<void> {
    62	    logger.info('Shutting down Storage Engine...');
    63	    await this.currentActiveStorage?.shutDown();
    64	    logger.info('Successfully shut down Storage Engine.');
    65	  }
    66	}
```

On `start()` (line 47), the engine looks up the provider matching `config.storage.type` (e.g. `"mongodb"` or `"postgresql"`) and calls `getStorage()` to create the active storage. The returned `BucketStorageFactory` is the main interface that the rest of the system uses.

Here's how the MongoDB storage module registers itself:

```bash
sed -n "7,37p" modules/module-mongodb-storage/src/module/MongoStorageModule.ts
```

```output
export class MongoStorageModule extends core.modules.AbstractModule {
  constructor() {
    super({
      name: 'MongoDB Bucket Storage'
    });
  }

  async initialize(context: core.system.ServiceContextContainer): Promise<void> {
    context.storageEngine.registerProvider(new MongoStorageProvider());

    if (types.isMongoStorageConfig(context.configuration.storage)) {
      context.migrations.registerMigrationAgent(
        new MongoMigrationAgent(this.resolveConfig(context.configuration.storage))
      );
    }
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: types.MongoStorageConfig) {
    return {
      ...config,
      ...lib_mongo.normalizeMongoConfig(config)
    };
  }

  async teardown(options: core.modules.TearDownOptions): Promise<void> {
    // teardown is implemented in the storage engine
  }
}
```

The `BucketStorageFactory` abstract class is the contract that both MongoDB and Postgres storage must implement. Its most important responsibilities:

- **`configureSyncRules()`** — deploy new sync rules if they've changed
- **`getActiveStorage()`** — return the current `SyncRulesBucketStorage` for serving sync requests
- **`getInstance()`** — get a storage instance for a specific sync rules version (needed during transitions)
- **`getReplicatingSyncRules()`** — list all sync rule versions currently being replicated

The key insight is that PowerSync can replicate into *two* sync rule versions simultaneously — the currently active one and a "next" version being prepared. This allows zero-downtime sync rule updates.

## 7. Sync Rules — The Query Language

Sync rules are the heart of PowerSync's data filtering. They define which rows from the source database end up in which client-side buckets. They're written in YAML with embedded SQL-like syntax and compiled by the `packages/sync-rules` package.

The sync rules package is substantial — let's see how many source files it has:

```bash
find packages/sync-rules/src -name "*.ts" | wc -l
```

```output
87
```

87 TypeScript files — this is the largest package in the monorepo. The key classes form a pipeline:

1. **`SqlSyncRules.fromYaml()`** — parses YAML sync rule definitions
2. **`SqlParameterQuery`** — compiles parameter queries that determine *which buckets* a user has access to (e.g., `SELECT org_id FROM users WHERE id = token_parameters.user_id`)
3. **`SqlDataQuery`** — compiles data queries that determine *which rows* go into each bucket (e.g., `SELECT * FROM documents WHERE org_id = bucket.org_id`)
4. **`HydratedSyncRules`** — a fully resolved sync rules instance with parsed queries ready for evaluation
5. **`BucketParameterQuerier`** — evaluates parameter queries against incoming JWT token data to produce the set of buckets for a user

The SQL is not executed against a real database — it's compiled into an evaluation engine that runs in-process against the replicated data. The compiler pipeline includes:

```bash
ls -1 packages/sync-rules/src/compiler/
```

```output
bucket_resolver.ts
compatibility.ts
compiler.ts
detect_dangerous_parameters.ts
equality.ts
expression.ts
filter.ts
filter_simplifier.ts
ir_to_sync_plan.ts
parser.ts
querier_graph.ts
rows.ts
scope.ts
sqlite.ts
table.ts
```

The compiler parses SQL into an AST, resolves table references, detects dangerous parameter patterns, builds filter expressions, and ultimately produces a "sync plan" — an optimized representation of which data flows where. The top-level `SqlSyncRules` class ties it all together:

```bash
sed -n "66,80p" packages/sync-rules/src/SqlSyncRules.ts
```

```output
export class SqlSyncRules extends SyncConfig {
  static validate(yaml: string, options: SyncRulesOptions): YamlError[] {
    try {
      const rules = this.fromYaml(yaml, options);
      return rules.errors;
    } catch (e) {
      if (e instanceof SyncRulesErrors) {
        return e.errors;
      } else if (e instanceof YamlError) {
        return [e];
      } else {
        return [new YamlError(e)];
      }
    }
  }
```

## 8. The Replication Engine — Change Data Capture

The replication engine captures changes from source databases and writes them into bucket storage. It's a three-layer abstraction:

1. **`ReplicationEngine`** — owns all replicators, starts/stops them together
2. **`AbstractReplicator`** — manages the lifecycle of one data source connection, handles sync rule transitions
3. **`AbstractReplicationJob`** — performs the actual replication work for one version of sync rules

The replication engine is only created for SYNC and UNIFIED modes (not API-only). Here's how it's registered:

```bash
sed -n "9,18p" service/src/runners/stream-worker.ts
```

```output
export const registerReplicationServices = (serviceContext: core.system.ServiceContextContainer) => {
  // Needs to be executed after shared registrations
  const replication = new core.replication.ReplicationEngine();

  serviceContext.register(core.replication.ReplicationEngine, replication);
  serviceContext.lifeCycleEngine.withLifecycle(replication, {
    start: (replication) => replication.start(),
    stop: (replication) => replication.shutDown()
  });
};
```

Now let's look at the ReplicationEngine itself — it's surprisingly simple because all the complexity lives in the replicators it manages:

```bash
sed -n "5,43p" packages/service-core/src/replication/ReplicationEngine.ts
```

```output
export class ReplicationEngine {
  private readonly replicators: Map<string, AbstractReplicator> = new Map();
  private probeInterval: NodeJS.Timeout | null = null;

  /**
   *  Register a Replicator with the engine
   *
   *  @param replicator
   */
  public register(replicator: AbstractReplicator) {
    if (this.replicators.has(replicator.id)) {
      throw new Error(`Replicator with id: ${replicator.id} already registered`);
    }
    logger.info(`Successfully registered Replicator ${replicator.id} with ReplicationEngine`);
    this.replicators.set(replicator.id, replicator);
  }

  /**
   *  Start replication on all managed Replicators
   */
  public start(): void {
    logger.info('Starting Replication Engine...');
    for (const replicator of this.replicators.values()) {
      logger.info(`Starting Replicator: ${replicator.id}`);
      replicator.start();
    }
    if (this.replicators.size == 0) {
      // If a replicator is running, the replicators update the probes.
      // If no connections are configured, then no replicator is running
      // Typical when no connections are configured.
      // In this case, update the probe here to avoid liveness probe failures.
      this.probeInterval = setInterval(() => {
        container.probes.touch().catch((e) => {
          logger.error(`Failed to touch probe`, e);
        });
      }, 5_000);
    }
    logger.info('Successfully started Replication Engine.');
  }
```

The `AbstractReplicator` contains the most sophisticated logic — its `runLoop()` method runs continuously, managing sync rule transitions. This is where the "two-version" sync rule deployment happens:

```bash
sed -n "133,182p" packages/service-core/src/replication/AbstractReplicator.ts
```

```output
  private async runLoop() {
    const syncRules = await this.syncRuleProvider.get();

    let configuredLock: storage.ReplicationLock | undefined = undefined;
    if (syncRules != null) {
      this.logger.info('Loaded sync rules');
      try {
        // Configure new sync rules, if they have changed.
        // In that case, also immediately take out a lock, so that another process doesn't start replication on it.

        const { lock } = await this.storage.configureSyncRules(
          storage.updateSyncRulesFromYaml(syncRules, { lock: true, validate: this.syncRuleProvider.exitOnError })
        );
        if (lock) {
          configuredLock = lock;
        }
      } catch (e) {
        // Log and re-raise to exit.
        // Should only reach this due to validation errors if exit_on_error is true.
        this.logger.error(`Failed to update sync rules from configuration`, e);
        throw e;
      }
    } else {
      this.logger.info('No sync rules configured - configure via API');
    }
    while (!this.stopped) {
      await container.probes.touch();
      try {
        await this.refresh({ configured_lock: configuredLock });
        // The lock is only valid on the first refresh.
        configuredLock = undefined;

        // Ensure that the replication jobs' connections are kept alive.
        // We don't ping while in error retry back-off, to avoid having too failures.
        if (this.rateLimiter.mayPing()) {
          const now = hrtime.bigint();
          if (now - this.lastPing >= PING_INTERVAL) {
            for (const activeJob of this.replicationJobs.values()) {
              await activeJob.keepAlive();
            }

            this.lastPing = now;
          }
        }
      } catch (e) {
        this.logger.error('Failed to refresh replication jobs', e);
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
```

The run loop:

1. **Loads sync rules** from config and deploys them if changed, immediately taking a distributed lock
2. **Enters a 5-second refresh loop** that:
   - Calls `refresh()` to reconcile running jobs with the sync rules in storage
   - Creates new `ReplicationJob`s for new sync rule versions
   - Stops orphaned jobs whose sync rules have been superseded
   - Sends keepalive pings to maintain database connections
   - Touches health probes to avoid liveness failures

The `refresh()` method (not shown in full) handles the delicate dance of running two sync rule versions concurrently. When new sync rules are deployed, a new job starts replicating the full snapshot while the old job continues serving live queries. Once the new job catches up, the old one is terminated and its storage is cleaned up asynchronously.

## 9. Postgres WAL Replication — A Deep Dive

Let's trace how the Postgres module actually captures changes. The `PostgresModule` creates a `WalStreamReplicator`, which in turn creates `WalStreamReplicationJob`s:

```bash
sed -n "23,54p" modules/module-postgres/src/module/PostgresModule.ts
```

```output
export class PostgresModule extends replication.ReplicationModule<types.PostgresConnectionConfig> {
  constructor() {
    super({
      name: 'Postgres',
      type: types.POSTGRES_CONNECTION_TYPE,
      configSchema: types.PostgresConnectionConfig
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {
    // Record replicated bytes using global jpgwire metrics. Only registered if this module is replicating
    if (context.replicationEngine) {
      jpgwire.setMetricsRecorder({
        addBytesRead(bytes) {
          context.metricsEngine.getCounter(ReplicationMetric.DATA_REPLICATED_BYTES).add(bytes);
        }
      });
      this.logger.info('Successfully set up connection metrics recorder for PostgresModule.');
    }
  }

  protected createRouteAPIAdapter(): api.RouteAPI {
    return PostgresRouteAPIAdapter.withConfig(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig);

    return new WalStreamReplicator({
      id: this.getDefaultId(normalisedConfig.database),
```

The `PostgresModule` extends `ReplicationModule`, a base class that handles config validation and the connection between modules and the replication engine. Notice that `createReplicator()` (line 49) also registers a `RouteAPIAdapter` — this provides database-specific logic for the sync API endpoints (like looking up source table schemas).

The `WalStreamReplicator` creates `WalStreamReplicationJob`s. Each job uses a `WalStream` to consume Postgres logical replication. The job's `replicate()` method:

```bash
sed -n "51,105p" modules/module-postgres/src/replication/WalStreamReplicationJob.ts
```

```output
  async replicate() {
    try {
      await this.replicateOnce();
    } catch (e) {
      // Fatal exception

      if (!this.isStopped) {
        // Ignore aborted errors

        this.logger.error(`Replication error`, e);
        if (e.cause != null) {
          // Example:
          // PgError.conn_ended: Unable to do postgres query on ended connection
          //     at PgConnection.stream (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:315:13)
          //     at stream.next (<anonymous>)
          //     at PgResult.fromStream (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:1174:22)
          //     at PgConnection.query (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:311:21)
          //     at WalStream.startInitialReplication (file:///.../powersync/powersync-service/lib/replication/WalStream.js:266:22)
          //     ...
          //   cause: TypeError: match is not iterable
          //       at timestamptzToSqlite (file:///.../powersync/packages/jpgwire/dist/util.js:140:50)
          //       at PgType.decode (file:///.../powersync/packages/jpgwire/dist/pgwire_types.js:25:24)
          //       at PgConnection._recvDataRow (file:///.../powersync/packages/jpgwire/dist/util.js:88:22)
          //       at PgConnection._recvMessages (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:656:30)
          //       at PgConnection._ioloopAttempt (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:563:20)
          //       at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
          //       at async PgConnection._ioloop (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:517:14),
          //   [Symbol(pg.ErrorCode)]: 'conn_ended',
          //   [Symbol(pg.ErrorResponse)]: undefined
          // }
          // Without this additional log, the cause would not be visible in the logs.
          this.logger.error(`cause`, e.cause);
        }
        // Report the error if relevant, before retrying
        container.reporter.captureException(e, {
          metadata: {
            replication_slot: this.slotName
          }
        });
        // This sets the retry delay
        this.rateLimiter.reportError(e);
      }

      if (e instanceof MissingReplicationSlotError) {
        if (shouldRetryReplication(e)) {
          // This stops replication on this slot and restarts with a new slot
          await this.options.storage.factory.restartReplication(this.storage.group_id);
        }
      }

      // No need to rethrow - the error is already logged, and retry behavior is the same on error
    } finally {
      this.abortController.abort();
    }
  }
```

The error handling here is production-hardened:

- Errors are logged with their full cause chain (lines 63-82)
- They're reported to Sentry (line 85)
- The rate limiter records the error to implement exponential backoff (line 90)
- A special `MissingReplicationSlotError` triggers slot recreation — this handles cases where Postgres drops the replication slot (e.g., during failover)

The `WalStream` class itself (in `WalStream.ts`, ~1000 lines) handles two phases:

1. **Initial snapshot** — queries all tables matching the sync rules and copies them row-by-row into bucket storage
2. **Streaming replication** — consumes the Postgres WAL (write-ahead log) via logical decoding, processing INSERT/UPDATE/DELETE events in real-time

It uses the custom `jpgwire` package for the Postgres wire protocol, which is a fork of pgwire with PowerSync-specific type handling.

```bash
sed -n "86,103p" modules/module-postgres/src/replication/WalStream.ts
```

```output
  needsNewSlot: boolean;
}

export const ZERO_LSN = '00000000/00000000';
export const PUBLICATION_NAME = 'powersync';
export const POSTGRES_DEFAULT_SCHEMA = 'public';

export const KEEPALIVE_CONTENT = 'ping';
export const KEEPALIVE_BUFFER = Buffer.from(KEEPALIVE_CONTENT);
export const KEEPALIVE_STATEMENT: pgwire.Statement = {
  statement: /* sql */ `
    SELECT
      *
    FROM
      pg_logical_emit_message(FALSE, 'powersync', $1)
  `,
  params: [{ type: 'varchar', value: KEEPALIVE_CONTENT }]
} as const;
```

The keepalive mechanism (line 95-102) is notable: it writes a logical replication message to Postgres WAL. This is needed because on RDS, Postgres creates new 64MB WAL files every 5 minutes, and without activity, the replication slot doesn't advance — causing WAL accumulation. The keepalive ping ensures the slot stays current.

## 10. HTTP Routing — Fastify and RSocket

The `CoreModule` is responsible for setting up the HTTP infrastructure. It configures both a Fastify HTTP server and an RSocket reactive-stream server on the same port:

```bash
sed -n "35,45p" modules/module-core/src/CoreModule.ts
```

```output
  protected registerAPIRoutes(context: core.ServiceContextContainer) {
    context.routerEngine.registerRoutes({
      api_routes: [
        ...core.routes.endpoints.ADMIN_ROUTES,
        ...core.routes.endpoints.CHECKPOINT_ROUTES,
        ...core.routes.endpoints.SYNC_RULES_ROUTES
      ],
      stream_routes: [...core.routes.endpoints.SYNC_STREAM_ROUTES],
      socket_routes: [core.routes.endpoints.syncStreamReactive]
    });
  }
```

Routes are split into three categories:

- **`api_routes`** — admin endpoints, checkpoint writes, sync rules management (concurrency: 10)
- **`stream_routes`** — the `/sync/stream` endpoint for long-lived sync connections (concurrency: 200)
- **`socket_routes`** — RSocket reactive-stream endpoints

The split exists so that admin API calls aren't blocked by a flood of sync streams, and vice versa. Each category gets its own Fastify encapsulated context with separate concurrency limits:

```bash
sed -n "38,53p" packages/service-core/src/routes/configure-fastify.ts
```

```output
export const DEFAULT_ROUTE_OPTIONS = {
  api: {
    routes: [...ADMIN_ROUTES, ...CHECKPOINT_ROUTES, ...SYNC_RULES_ROUTES, ...PROBES_ROUTES],
    queue_options: {
      concurrency: 10,
      max_queue_depth: 20
    }
  },
  sync_stream: {
    routes: [...SYNC_STREAM_ROUTES],
    queue_options: {
      concurrency: 200,
      max_queue_depth: 0
    }
  }
};
```

The Fastify server is created inside the CoreModule's `configureRouterImplementation()`. Here you can see the full server setup — CORS, route registration, RSocket wiring, and the listen call:

```bash
sed -n "50,106p" modules/module-core/src/CoreModule.ts
```

```output
  protected configureRouterImplementation(context: core.ServiceContextContainer) {
    context.lifeCycleEngine.withLifecycle(context.routerEngine, {
      start: async (routerEngine) => {
        // The router engine will only start servers if routes have been registered
        await routerEngine.start(async (routes) => {
          const server = fastify.fastify();

          server.register(cors, {
            origin: '*',
            allowedHeaders: ['Content-Type', 'Authorization', 'User-Agent', 'X-User-Agent'],
            exposedHeaders: ['Content-Type'],
            // Cache time for preflight response
            maxAge: 3600
          });

          core.routes.configureFastifyServer(server, {
            service_context: context,
            routes: {
              api: { routes: routes.api_routes },
              sync_stream: {
                routes: routes.stream_routes,
                queue_options: {
                  concurrency: context.configuration.api_parameters.max_concurrent_connections,
                  max_queue_depth: 0
                }
              }
            }
          });

          const socketRouter = new ReactiveSocketRouter<core.routes.Context>({
            max_concurrent_connections: context.configuration.api_parameters.max_concurrent_connections
          });

          core.routes.configureRSocket(socketRouter, {
            server: server.server,
            service_context: context,
            route_generators: routes.socket_routes
          });

          const { port } = context.configuration;

          await server.listen({
            host: '0.0.0.0',
            port
          });

          framework.logger.info(`Running on port ${port}`);

          return {
            onShutdown: async () => {
              framework.logger.info('Shutting down HTTP server...');
              await server.close();
              framework.logger.info('HTTP server stopped');
            }
          };
        });
      }
```

Key details:

- CORS is configured with `origin: '*'` (line 58) — wide open, as the service is typically behind a reverse proxy
- The sync stream concurrency limit comes from `api_parameters.max_concurrent_connections` in config (line 73)
- The RSocket router shares the same underlying Node.js HTTP server (line 85) — WebSocket upgrades go to RSocket, regular HTTP goes to Fastify
- The router returns an `onShutdown` callback that closes the server gracefully

## 11. Authentication — JWT Verification

Every sync stream request must carry a valid JWT. The auth system lives in `packages/service-core/src/auth/` and is designed for flexibility — it supports JWKS endpoints, static keys, and Supabase-specific key discovery.

The sync stream endpoint uses the `authUser` middleware:

```bash
cat -n packages/service-core/src/routes/auth.ts
```

```output
     1	import { AuthorizationError, AuthorizationResponse, ErrorCode } from '@powersync/lib-services-framework';
     2	import * as auth from '../auth/auth-index.js';
     3	import { ServiceContext } from '../system/ServiceContext.js';
     4	import { BasicRouterRequest, Context, RequestEndpointHandlerPayload } from './router.js';
     5	
     6	export function endpoint(req: BasicRouterRequest) {
     7	  const protocol = req.headers['x-forwarded-proto'] ?? req.protocol;
     8	  const host = req.hostname;
     9	  return `${protocol}://${host}`;
    10	}
    11	
    12	export function getTokenFromHeader(authHeader: string = ''): string | null {
    13	  const tokenMatch = /^(Token|Bearer) (\S+)$/.exec(authHeader);
    14	  if (!tokenMatch) {
    15	    return null;
    16	  }
    17	  const token = tokenMatch[2];
    18	  return token ?? null;
    19	}
    20	
    21	export const authUser = async (payload: RequestEndpointHandlerPayload): Promise<AuthorizationResponse> => {
    22	  return authorizeUser(payload.context, payload.request.headers.authorization as string);
    23	};
    24	
    25	export async function authorizeUser(context: Context, authHeader: string = ''): Promise<AuthorizationResponse> {
    26	  const token = getTokenFromHeader(authHeader);
    27	  if (token == null) {
    28	    return {
    29	      authorized: false,
    30	      error: new AuthorizationError(ErrorCode.PSYNC_S2106, 'Authentication required')
    31	    };
    32	  }
    33	
    34	  const { context: tokenContext, tokenError } = await generateContext(context.service_context, token);
    35	
    36	  if (!tokenContext) {
    37	    return {
    38	      authorized: false,
    39	      error: tokenError
    40	    };
    41	  }
    42	
    43	  Object.assign(context, tokenContext);
    44	  return { authorized: true };
    45	}
    46	
    47	export async function generateContext(serviceContext: ServiceContext, token: string) {
    48	  const { configuration } = serviceContext;
    49	
    50	  try {
    51	    const maxAge = configuration.token_max_expiration;
    52	    const parsedToken = await configuration.client_keystore.verifyJwt(token, {
    53	      defaultAudiences: configuration.jwt_audiences,
    54	      maxAge: maxAge
    55	    });
    56	    return {
    57	      context: {
    58	        token_payload: parsedToken
    59	      }
    60	    };
    61	  } catch (err) {
    62	    return {
    63	      context: null,
    64	      tokenError: auth.mapAuthError(err, token)
    65	    };
    66	  }
    67	}
    68	
    69	export const authApi = (payload: RequestEndpointHandlerPayload) => {
    70	  const {
    71	    context: {
    72	      service_context: { configuration }
    73	    }
    74	  } = payload;
    75	  const api_keys = configuration.api_tokens;
    76	  if (api_keys.length == 0) {
    77	    return {
    78	      authorized: false,
    79	      errors: ['Authentication disabled']
    80	    };
    81	  }
    82	  const auth = (payload.request.headers.authorization as string) ?? '';
    83	
    84	  const tokenMatch = /^(Token|Bearer) (\S+)$/.exec(auth);
    85	  if (!tokenMatch) {
    86	    return {
    87	      authorized: false,
    88	      errors: ['Authentication required']
    89	    };
    90	  }
    91	  const token = tokenMatch[2];
    92	  if (api_keys.includes(token)) {
    93	    return { authorized: true };
    94	  } else {
    95	    return {
    96	      authorized: false,
    97	      errors: ['Authentication failed']
    98	    };
    99	  }
   100	};
```

There are two auth paths:

- **`authUser`** (line 21) — for sync streams. Extracts a Bearer token, verifies the JWT signature against the configured `client_keystore`, checks audience and expiration. The verified `JwtPayload` (including `user_id` and custom parameters) is attached to the request context.
- **`authApi`** (line 69) — for admin endpoints. Simple bearer-token-against-a-list-of-API-keys check.

The `KeyStore` (in `auth/KeyStore.ts`) handles the complexity of key management — it supports keys with and without key IDs (`kid`), multiple algorithms, JWKS auto-discovery, and Supabase-specific flows. The key matching strategy:
1. If the JWT has a `kid`, find the key with that `kid` (fast path)
2. Otherwise, try all "wildcard" keys (keys without `kid`) until one verifies

## 12. The Sync Stream Endpoint — Where Client Meets Server

This is the main endpoint that clients call to receive data. A client sends a `POST /sync/stream` with a JWT and a request body describing what it already has. The server responds with an NDJSON or BSON stream that never ends (until the client disconnects or the token expires).

Let's trace the handler:

```bash
sed -n "23,44p" packages/service-core/src/routes/endpoints/sync-stream.ts
```

```output
export const syncStreamed = routeDefinition({
  path: SyncRoutes.STREAM,
  method: router.HTTPMethod.POST,
  authorize: authUser,
  validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { service_context, logger, token_payload } = payload.context;
    const { routerEngine, storageEngine, metricsEngine, syncContext } = service_context;
    const headers = payload.request.headers;
    const userAgent = headers['x-user-agent'] ?? headers['user-agent'];
    const clientId = payload.params.client_id;
    const streamStart = Date.now();
    const negotiator = new Negotiator(payload.request);
    // This falls back to JSON unless there's preference for the bson-stream in the Accept header.
    const useBson = payload.request.headers.accept
      ? negotiator.mediaType(supportedContentTypes) == concatenatedBsonContentType
      : false;

    logger.defaultMeta = {
      ...logger.defaultMeta,
      user_agent: userAgent,
      client_id: clientId,
```

```bash
sed -n "64,110p" packages/service-core/src/routes/endpoints/sync-stream.ts
```

```output

    const bucketStorage = await storageEngine.activeBucketStorage.getActiveStorage();

    if (bucketStorage == null) {
      throw new errors.ServiceError({
        status: 500,
        code: ErrorCode.PSYNC_S2302,
        description: 'No sync rules available'
      });
    }

    const syncRules = bucketStorage.getParsedSyncRules(routerEngine.getAPI().getParseSyncRulesOptions());

    const controller = new AbortController();
    const tracker = new sync.RequestTracker(metricsEngine);

    const formattedAppMetadata = payload.params.app_metadata
      ? limitParamsForLogging(payload.params.app_metadata)
      : undefined;

    logger.info('Sync stream started', {
      app_metadata: formattedAppMetadata,
      client_params: payload.params.parameters ? limitParamsForLogging(payload.params.parameters) : undefined
    });

    try {
      metricsEngine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS).add(1);
      service_context.eventsEngine.emit(event_types.EventsEngineEventType.SDK_CONNECT_EVENT, sdkData);
      const syncLines = sync.streamResponse({
        syncContext: syncContext,
        bucketStorage,
        syncRules,
        params: payload.params,
        token: payload.context.token_payload!,
        tracker,
        signal: controller.signal,
        logger,
        isEncodingAsBson: useBson
      });

      const byteContents = useBson ? sync.bsonLines(syncLines) : sync.ndjson(syncLines);
      const plainStream = Readable.from(sync.transformToBytesTracked(byteContents, tracker), {
        objectMode: false,
        highWaterMark: 16 * 1024
      });
      const { stream, encodingHeaders } = maybeCompressResponseStream(negotiator, plainStream, tracker);

```

The handler flow:

1. **Content negotiation** (line 36-38): Clients can request BSON (`application/vnd.powersync.bson-stream`) or NDJSON (`application/x-ndjson`). BSON is more efficient for binary data.
2. **Get active storage** (line 64): Retrieves the current `SyncRulesBucketStorage` instance (there might be a newer version being prepared, but clients always get the active one).
3. **Parse sync rules** (line 74): Hydrates the sync rules with database-specific options (like the default schema name).
4. **Create sync stream** (line 92-101): Calls `sync.streamResponse()` — an async generator that yields sync lines indefinitely.
5. **Encode output** (line 103): Converts sync line objects to BSON or NDJSON bytes.
6. **Compress** (line 108): Optionally applies gzip/deflate compression based on `Accept-Encoding`.
7. **Return streaming response** (line 110+): Returns a 200 with the stream as the response body, plus an `X-Accel-Buffering: no` header to prevent nginx buffering.

The concurrent connection counter (line 90) is critical for monitoring — it tracks how many sync streams are active at any given moment.

## 13. The Sync Protocol — Checkpoints, Buckets, and Data Batches

Now we're at the heart of the sync protocol. The `streamResponse()` function in `packages/service-core/src/sync/sync.ts` is the async generator that produces the actual sync stream. Let's break it down:

```bash
sed -n "34,92p" packages/service-core/src/sync/sync.ts
```

```output
export async function* streamResponse(
  options: SyncStreamParameters
): AsyncIterable<util.StreamingSyncLine | string | null> {
  const {
    syncContext,
    bucketStorage,
    syncRules,
    params,
    token,
    tokenStreamOptions,
    tracker,
    signal,
    isEncodingAsBson
  } = options;
  const logger = options.logger ?? defaultLogger;

  // We also need to be able to abort, so we create our own controller.
  const controller = new AbortController();
  if (signal) {
    signal.addEventListener(
      'abort',
      () => {
        controller.abort();
      },
      { once: true }
    );
    if (signal.aborted) {
      controller.abort();
    }
  }
  const ki = tokenStream(token, controller.signal, tokenStreamOptions);
  const stream = streamResponseInner(
    syncContext,
    bucketStorage,
    syncRules,
    params,
    token,
    tracker,
    controller.signal,
    logger,
    isEncodingAsBson
  );
  // Merge the two streams, and abort as soon as one of the streams end.
  const merged = mergeAsyncIterables([stream, ki], controller.signal);

  try {
    yield* merged;
  } catch (e) {
    if (e instanceof AbortError) {
      return;
    } else {
      throw e;
    }
  } finally {
    // This ensures all the underlying streams are aborted as soon as possible if the
    // parent loop stops.
    controller.abort();
  }
}
```

Two async iterables are merged (line 77):

1. **`tokenStream`** (`ki`) — a timer that fires when the JWT token is about to expire, forcing the stream to close so the client must re-authenticate
2. **`streamResponseInner`** — the actual data stream

If either stream ends, the `AbortController` aborts both. This is the "token expiry" mechanism — no long-lived connection can outlast its JWT.

The inner stream function is where the real protocol logic lives:

```bash
sed -n "94,145p" packages/service-core/src/sync/sync.ts
```

```output
async function* streamResponseInner(
  syncContext: SyncContext,
  bucketStorage: storage.SyncRulesBucketStorage,
  syncRules: HydratedSyncRules,
  params: util.StreamingSyncRequest,
  tokenPayload: auth.JwtPayload,
  tracker: RequestTracker,
  signal: AbortSignal,
  logger: Logger,
  isEncodingAsBson: boolean
): AsyncGenerator<util.StreamingSyncLine | string | null> {
  const checkpointUserId = util.checkpointUserId(tokenPayload.userIdString, params.client_id);

  const checksumState = new BucketChecksumState({
    syncContext,
    bucketStorage,
    syncRules,
    tokenPayload,
    syncRequest: params,
    logger: logger
  });
  const stream = bucketStorage.watchCheckpointChanges({
    user_id: checkpointUserId,
    signal
  });
  const newCheckpoints = stream[Symbol.asyncIterator]();

  type CheckpointAndLine = {
    checkpoint: bigint;
    line: CheckpointLine | null;
  };

  async function waitForNewCheckpointLine(): Promise<IteratorResult<CheckpointAndLine>> {
    const next = await newCheckpoints.next();
    if (next.done) {
      return { done: true, value: undefined };
    }

    const line = await checksumState.buildNextCheckpointLine(next.value);
    return { done: false, value: { checkpoint: next.value.base.checkpoint, line } };
  }

  try {
    let nextCheckpointPromise: Promise<PromiseSettledResult<IteratorResult<CheckpointAndLine>>> | undefined;

    do {
      if (!nextCheckpointPromise) {
        // Wrap in a settledPromise, so that abort errors after the parent stopped iterating
        // does not result in uncaught errors.
        nextCheckpointPromise = settledPromise(waitForNewCheckpointLine());
      }
      const next = await nextCheckpointPromise;
```

The inner stream:

1. **Watches for checkpoint changes** (line 116-119): `bucketStorage.watchCheckpointChanges()` is a long-polling or notification-based async iterable. Every time new data is committed to bucket storage (by the replication engine), a new checkpoint is emitted.

2. **Builds checkpoint lines** (line 133): `BucketChecksumState.buildNextCheckpointLine()` compares the new checkpoint against what the client already has (from the request's `buckets` parameter) and computes which buckets have changed checksums.

3. **Sends data by priority** (lines 168-242 below): Buckets are grouped by priority and sent from highest to lowest. This ensures critical data reaches the client first.

Let's see the priority-based data sending:

```bash
sed -n "168,250p" packages/service-core/src/sync/sync.ts
```

```output
      // the new checkpoint.
      const abortCheckpointController = new AbortController();
      let syncedOperations = 0;

      const abortCheckpointSignal = AbortSignal.any([abortCheckpointController.signal, signal]);

      const bucketsByPriority = [...Map.groupBy(bucketsToFetch, (bucket) => bucket.priority).entries()];
      bucketsByPriority.sort((a, b) => a[0] - b[0]); // Sort from high to lower priorities
      const lowestPriority = bucketsByPriority.at(-1)?.[0];

      // Ensure that we have at least one priority batch: After sending the checkpoint line, clients expect to
      // receive a sync complete message after the synchronization is done (which happens in the last
      // bucketDataInBatches iteration). Without any batch, the line is missing and clients might not complete their
      // sync properly.
      const priorityBatches: [BucketPriority | null, ResolvedBucket[]][] = bucketsByPriority;
      if (priorityBatches.length == 0) {
        priorityBatches.push([null, []]);
      }

      function maybeRaceForNewCheckpoint() {
        if (syncedOperations >= 1000 && nextCheckpointPromise === undefined) {
          nextCheckpointPromise = (async () => {
            while (true) {
              const next = await settledPromise(waitForNewCheckpointLine());
              if (next.status == 'rejected') {
                abortCheckpointController.abort();
              } else if (!next.value.done) {
                if (next.value.value.line == null) {
                  // There's a new checkpoint that doesn't affect this sync stream. Keep listening, but don't
                  // interrupt this batch.
                  continue;
                }

                // A new sync line can be emitted. Stop running the bucketDataInBatches() iterations, making the
                // main flow reach the new checkpoint.
                abortCheckpointController.abort();
              }

              return next;
            }
          })();
        }
      }

      function markOperationsSent(stats: OperationsSentStats) {
        syncedOperations += stats.total;
        tracker.addOperationsSynced(stats);
        maybeRaceForNewCheckpoint();
      }

      // This incrementally updates dataBuckets with each individual bucket position.
      // At the end of this, we can be sure that all buckets have data up to the checkpoint.
      for (const [priority, buckets] of priorityBatches) {
        const isLast = priority === lowestPriority;
        if (abortCheckpointSignal.aborted) {
          break;
        }

        yield* bucketDataInBatches({
          syncContext: syncContext,
          bucketStorage: bucketStorage,
          checkpoint: next.value.value.checkpoint,
          bucketsToFetch: buckets,
          checkpointLine: line,
          legacyDataLines: !isEncodingAsBson && params.raw_data != true,
          onRowsSent: markOperationsSent,
          abort_connection: signal,
          abort_batch: abortCheckpointSignal,
          userIdForLogs: tokenPayload.userIdJson,
          // Passing null here will emit a full sync complete message at the end. If we pass a priority, we'll emit a partial
          // sync complete message instead.
          forPriority: !isLast ? priority : null,
          logger
        });
      }

      if (!abortCheckpointSignal.aborted) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    } while (!signal.aborted);
  } finally {
    await newCheckpoints.return?.();
  }
```

This is the most sophisticated part of the sync protocol. The key mechanisms:

**Priority-based delivery** (line 174-175): Buckets are grouped by priority and sorted high-to-low. High-priority data (e.g., user profile) is sent before low-priority data (e.g., historical logs). For non-final priorities, a `partial_checkpoint_complete` message is sent so clients can start using that data immediately.

**Checkpoint racing** (lines 188-210): After sending 1000+ operations, the system starts listening for new checkpoints *concurrently* with sending lower-priority data. If a new checkpoint arrives (meaning the source database changed again), it aborts the current batch and jumps to the new checkpoint. This ensures clients get the freshest data possible rather than completing a stale checkpoint.

**Semaphore-based concurrency** (inside `bucketDataBatch`): The `syncContext.syncSemaphore` limits how many sync streams can fetch bucket data simultaneously, preventing the storage backend from being overwhelmed.

The final message in each complete cycle is either:
- `checkpoint_complete` — all buckets are synced to this checkpoint
- `partial_checkpoint_complete` — buckets of a specific priority are synced

After this, the loop waits for the next checkpoint notification from the storage engine, and the cycle repeats.

## 14. Putting It All Together — The Full Data Path

Here's the complete path data takes through the system:

```
Source Database (Postgres/MongoDB/MySQL/MSSQL)
    │
    │  WAL stream / Change stream / CDC / Binlog
    ▼
ReplicationModule (e.g., PostgresModule)
    │
    │  creates WalStreamReplicator
    ▼
AbstractReplicator.runLoop()
    │
    │  manages sync rule versions, creates jobs
    ▼
AbstractReplicationJob.replicate()
    │
    │  initial snapshot + streaming changes
    ▼
WalStream / ChangeStream / CDCStream / BinLogStream
    │
    │  evaluates sync rules on each row
    ▼
BucketStorageBatch.save()
    │
    │  writes to bucket_data tables
    ▼
SyncRulesBucketStorage (MongoDB or Postgres)
    │
    │  emits checkpoint notifications
    ▼
watchCheckpointChanges()
    │
    │  picked up by sync stream
    ▼
streamResponseInner()
    │
    │  computes checksum diffs, fetches bucket data
    ▼
bucketDataInBatches()
    │
    │  yields StreamingSyncLine objects
    ▼
NDJSON / BSON encoder
    │
    │  optional gzip compression
    ▼
HTTP Response Stream → Client SDK → SQLite
```

The beauty of this architecture is the clean separation between the write side (replication) and the read side (sync streams). They communicate only through the storage layer — the replication engine writes data and advances checkpoints, the sync streams watch for checkpoint changes and read bucket data. This means:

- Replication and serving can run in separate processes (`SYNC` + `API` modes)
- Multiple API processes can serve the same storage
- Storage backends are fully pluggable
- New source databases can be added without touching the sync protocol

## 15. Key Supporting Packages

A few packages deserve mention for the infrastructure they provide:

**`packages/jpgwire`** — A custom fork of the pgwire Postgres wire protocol library. PowerSync needs raw WAL access for logical replication, which standard Postgres clients don't provide. jpgwire handles the binary protocol, type decoding (including custom types like `timestamptz`), and streaming query results.

**`packages/rsocket-router`** — Implements the RSocket protocol over WebSockets for reactive-stream sync. RSocket provides backpressure-aware streaming, which is important for mobile clients on slow connections.

**`packages/jsonbig`** — JSON serialization that preserves BigInt values. Postgres `bigint` columns and internal operation IDs use 64-bit integers that exceed JavaScript's `Number.MAX_SAFE_INTEGER`. This package ensures they survive serialization.

**`libs/lib-services`** — The microservice framework providing dependency injection (`container`), structured logging (Winston), health probes (filesystem and HTTP), migration management, and the `LifeCycledSystem` that orchestrates startup/shutdown order.

## Summary

The PowerSync Service is a well-structured TypeScript monorepo that implements a sophisticated real-time data synchronization engine. Its key architectural decisions:

1. **Module system** — database-specific code is loaded only when needed, keeping the core clean
2. **Two-phase sync rules** — seamless transitions between sync rule versions without downtime
3. **Priority-based streaming** — critical data reaches clients first
4. **Checkpoint racing** — fresh data interrupts stale batches
5. **Storage abstraction** — the same sync protocol works over MongoDB or Postgres storage
6. **Lifecycle management** — ordered startup and graceful shutdown prevent data loss
7. **Write/read separation** — replication and serving can scale independently

The codebase is approximately 650+ TypeScript source files across 20+ packages, but the linear path from boot to sync stream touches only about a dozen key files. Everything else is pluggable infrastructure supporting that core path.
