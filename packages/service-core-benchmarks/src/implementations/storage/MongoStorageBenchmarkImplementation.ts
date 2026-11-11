import { mongo, MONGO_SOCKET_TIMEOUT_MS } from '@powersync/lib-service-mongodb';
import { MongoBucketStorage, PowerSyncMongo } from '@powersync/service-module-mongodb-storage';
import {
  MongoStorageBenchmarkImplementationOptions,
  StorageBenchmarkImplementation,
  StorageBenchmarkRunResource
} from '../../types/StorageBenchmark.js';

export class MongoStorageBenchmarkImplementation implements StorageBenchmarkImplementation {
  readonly id = 'storage:mongodb' as const;

  constructor(private readonly options: MongoStorageBenchmarkImplementationOptions) {}

  async open(signal: AbortSignal): Promise<StorageBenchmarkRunResource> {
    signal.throwIfAborted();
    // Checkpoint watchers await events for up to 10 seconds. The test helper's
    // 5-second socket timeout can expire on a healthy, idle watcher.
    const database = new PowerSyncMongo(
      new mongo.MongoClient(this.options.url, {
        connectTimeoutMS: this.options.isCI ? 15_000 : 5_000,
        socketTimeoutMS: MONGO_SOCKET_TIMEOUT_MS,
        serverSelectionTimeoutMS: this.options.isCI ? 15_000 : 2_500
      })
    );
    let factory: MongoBucketStorage | undefined;

    try {
      if (!(await database.db.listCollections({ name: database.bucket_parameters.collectionName }).hasNext())) {
        await database.db.createCollection(database.bucket_parameters.collectionName);
      }
      await database.clear();
      await database.createCheckpointEventsCollection();
      factory = new MongoBucketStorage(database, {
        replicationStreamNamePrefix: 'benchmark_',
        supportsMultipleSyncConfigs: true
      });
      signal.throwIfAborted();
      const serverVersion = await readServerVersion(factory);
      const systemIdentifier = await factory.getSystemIdentifier();
      return {
        factory,
        tableIdStrings: false,
        environment: {
          implementation: this.id,
          server_version: serverVersion,
          system_identifier: systemIdentifier
        },
        async dispose() {
          await disposeMongoResources(factory, database);
        }
      };
    } catch (error) {
      try {
        await disposeMongoResources(factory, database);
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'MongoDB benchmark setup and cleanup failed');
      }
      throw error;
    }
  }
}

async function readServerVersion(factory: MongoBucketStorage): Promise<string> {
  const buildInfo = await factory.db.db.command({ buildInfo: 1 });
  if (typeof buildInfo.version !== 'string') {
    throw new Error('MongoDB did not return a server version');
  }
  return buildInfo.version;
}

async function disposeMongoResources(
  factory: MongoBucketStorage | undefined,
  database: MongoBucketStorage['db']
): Promise<void> {
  const errors: unknown[] = [];
  if (factory != null) {
    try {
      await factory[Symbol.asyncDispose]();
    } catch (error) {
      errors.push(error);
    }
  }
  try {
    await database.client.close();
  } catch (error) {
    errors.push(error);
  }
  if (errors.length > 0) {
    throw new AggregateError(errors, 'MongoDB benchmark cleanup failed');
  }
}
