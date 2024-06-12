import { locks } from '@journeyapps-platform/micro';
import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

import { Direction, createMongoMigrationStore, execute, writeLogsToStore } from '@journeyapps-platform/micro-migrate';

import * as db from '../db/db-index.js';
import * as util from '../util/util-index.js';

const DEFAULT_MONGO_LOCK_COLLECTION = 'locks';
const MONGO_LOCK_PROCESS = 'migrations';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const MIGRATIONS_DIR = path.join(__dirname, '/db/migrations');

export type MigrationOptions = {
  direction: Direction;
  runner_config: util.RunnerConfig;
};

/**
 * Loads migrations and injects a custom context for loading the specified
 * runner configuration.
 */
const loadMigrations = async (dir: string, runner_config: util.RunnerConfig) => {
  const files = await fs.readdir(dir);
  const migrations = files.filter((file) => {
    return path.extname(file) === '.js';
  });

  const context: util.MigrationContext = {
    runner_config
  };

  return await Promise.all(
    migrations.map(async (migration) => {
      const module = await import(path.resolve(dir, migration));
      return {
        name: path.basename(migration).replace(path.extname(migration), ''),
        up: () => module.up(context),
        down: () => module.down(context)
      };
    })
  );
};

/**
 * Runs migration scripts exclusively using Mongo locks
 */
export const migrate = async (options: MigrationOptions) => {
  const { direction, runner_config } = options;

  /**
   * Try and get Mongo from config file.
   * But this might not be available in Journey Micro as we use the standard Mongo.
   */

  const config = await util.loadConfig(runner_config);
  const { storage } = config;

  const client = db.mongo.createMongoClient(storage);
  await client.connect();

  const clientDB = client.db(storage.database);
  const collection = clientDB.collection<locks.Lock>(DEFAULT_MONGO_LOCK_COLLECTION);

  const manager = locks.createMongoLockManager(collection, {
    name: MONGO_LOCK_PROCESS
  });

  // Only one process should execute this at a time.
  const lockId = await manager.acquire();

  if (!lockId) {
    throw new Error('Could not acquire lock_id');
  }

  let isReleased = false;
  const releaseLock = async () => {
    if (isReleased) {
      return;
    }
    await manager.release(lockId);
    isReleased = true;
  };

  // For the case where the migration is terminated
  process.addListener('beforeExit', releaseLock);

  try {
    const migrations = await loadMigrations(MIGRATIONS_DIR, runner_config);

    // Use the provided config to connect to Mongo
    const store = createMongoMigrationStore({
      uri: storage.uri,
      database: storage.database,
      username: storage.username,
      password: storage.password
    });

    const state = await store.load();

    const logStream = execute({
      direction: direction,
      migrations,
      state
    });

    await writeLogsToStore({
      log_stream: logStream,
      store,
      state
    });
  } finally {
    await releaseLock();
    await client.close(true);
    process.removeListener('beforeExit', releaseLock);
  }
};
