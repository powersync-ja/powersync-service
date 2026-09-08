import { mongo } from '@powersync/lib-service-mongodb';
import { MongoBucketStorage, PowerSyncMongo, S3ObjectStorage } from '@powersync/service-module-mongodb-storage';
import { randomUUID } from 'node:crypto';
import {
  MongoStorageBenchmarkImplementationOptions,
  StorageBenchmarkImplementation,
  StorageBenchmarkRunResource
} from '../../types/StorageBenchmark.js';

export class MongoStorageBenchmarkImplementation implements StorageBenchmarkImplementation {
  readonly id = 'mongodb-storage' as const;

  constructor(private readonly options: MongoStorageBenchmarkImplementationOptions) {}

  async open(signal: AbortSignal): Promise<StorageBenchmarkRunResource> {
    signal.throwIfAborted();
    if (
      this.options.inlineThresholdBytes != null &&
      (!Number.isSafeInteger(this.options.inlineThresholdBytes) || this.options.inlineThresholdBytes < 0)
    )
      throw new Error('inlineThresholdBytes must be a non-negative integer');
    const name = `powersync_benchmark_${randomUUID().replaceAll('-', '')}`;
    const database = new PowerSyncMongo(
      new mongo.MongoClient(this.options.url, {
        serverSelectionTimeoutMS: 30_000,
        socketTimeoutMS: 120_000
      }),
      { database: name }
    );
    const objectStorage =
      this.options.objectStorage == null
        ? undefined
        : new S3ObjectStorage({
            ...this.options.objectStorage,
            prefix: name
          });
    let uploads = 0;
    let bytes = 0;
    if (objectStorage) {
      const put = objectStorage.put.bind(objectStorage);
      objectStorage.put = async (...args: Parameters<typeof put>) => {
        await put(...args);
        uploads++;
        bytes += args[1].byteLength;
      };
    }
    const required = this.options.inlineThresholdBytes === 0;
    let factory: MongoBucketStorage | undefined;

    try {
      if (!(await database.db.listCollections({ name: database.bucket_parameters.collectionName }).hasNext())) {
        await database.db.createCollection(database.bucket_parameters.collectionName);
      }
      await database.createCheckpointEventsCollection();
      factory = new MongoBucketStorage(database, {
        replicationStreamNamePrefix: 'benchmark_',
        supportsMultipleSyncConfigs: true,
        objectStorage,
        inlineThresholdBytes: this.options.inlineThresholdBytes
      });
      signal.throwIfAborted();
      const serverVersion = await readServerVersion(factory);
      const systemIdentifier = await factory.getSystemIdentifier();
      return {
        factory,
        tableIdStrings: false,
        objectStorageMetrics: objectStorage ? () => ({ uploads, bytes, required }) : undefined,
        environment: {
          implementation: this.id,
          server_version: serverVersion,
          system_identifier: systemIdentifier,
          storage_database: name,
          object_storage: objectStorage != null,
          inline_threshold_bytes: this.options.inlineThresholdBytes
        },
        async dispose() {
          await disposeMongoResources(factory, database, objectStorage);
        }
      };
    } catch (error) {
      try {
        await disposeMongoResources(factory, database, objectStorage);
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
  database: MongoBucketStorage['db'],
  objectStorage?: S3ObjectStorage
): Promise<void> {
  const errors: unknown[] = [];
  try {
    await database.db.dropDatabase();
    await objectStorage?.deletePrefix('');
  } catch (error) {
    errors.push(error);
  } finally {
    objectStorage?.client.destroy();
  }
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
