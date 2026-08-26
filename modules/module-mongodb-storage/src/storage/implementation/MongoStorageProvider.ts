import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { POWERSYNC_VERSION, storage } from '@powersync/service-core';
import { MongoStorageConfig, normalizeClearBatchThrottleRate } from '../../types/types.js';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { MongoReportStorage } from '../MongoReportStorage.js';
import { PowerSyncMongo } from './db.js';
import type { ObjectStorage } from './v3/object-storage/ObjectStorage.js';

export class MongoStorageProvider implements storage.StorageProvider {
  get type() {
    return lib_mongo.MONGO_CONNECTION_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { resolvedConfig } = options;

    const { storage } = resolvedConfig;
    if (storage.type != this.type) {
      // This should not be reached since the generation should be managed externally.
      throw new ServiceAssertionError(
        `Cannot create MongoDB bucket storage with provided config ${storage.type} !== ${this.type}`
      );
    }

    const decodedConfig = MongoStorageConfig.decode(storage as any);

    let objectStorage: ObjectStorage | undefined;
    if (decodedConfig.object_storage?.type === 's3') {
      // Dynamically import S3ObjectStorage to avoid loading AWS SDK unless needed.
      const { S3ObjectStorage } = await import('./v3/object-storage/S3ObjectStorage.js');
      objectStorage = new S3ObjectStorage({
        bucket: decodedConfig.object_storage.bucket,
        region: decodedConfig.object_storage.region,
        prefix: decodedConfig.object_storage.prefix,
        endpoint: decodedConfig.object_storage.endpoint,
        forcePathStyle: decodedConfig.object_storage.force_path_style,
        accessKeyId: decodedConfig.object_storage.access_key_id,
        secretAccessKey: decodedConfig.object_storage.secret_access_key,
        concurrencyLimit: decodedConfig.object_storage.concurrency_limit,
        defaultsMode: decodedConfig.object_storage.defaults_mode
      });
    }

    const client = lib_mongo.db.createMongoClient(decodedConfig, {
      powersyncVersion: POWERSYNC_VERSION,
      maxPoolSize: resolvedConfig.storage.max_pool_size ?? 8
    });

    let shuttingDown = false;

    // Explicitly connect on startup.
    // Connection errors during startup are typically not recoverable - we get topologyClosed.
    // This helps to catch the error early, along with the cause, and before the process starts
    // to serve API requests.
    // Errors here will cause the process to exit.
    await client.connect();

    const database = new PowerSyncMongo(client, { database: resolvedConfig.storage.database });
    const readPreference =
      decodedConfig.bulk_read_preference == null
        ? undefined
        : new lib_mongo.mongo.ReadPreference(decodedConfig.bulk_read_preference, undefined, {
            // maxStalenessSeconds is relevant for all modes except 'primary'.
            // 90 is the minimum value.
            maxStalenessSeconds: decodedConfig.bulk_read_preference == 'primary' ? undefined : 90
          });
    const syncStorageFactory = new MongoBucketStorage(database, {
      replicationStreamNamePrefix: resolvedConfig.slot_name_prefix,
      readPreference,
      clearBatchThrottleRate: normalizeClearBatchThrottleRate(decodedConfig.clear_batch_throttle_rate),
      checksumCacheTtlMs: resolvedConfig.api_parameters.bucket_count_cache_ttl_minutes * 60_000,
      defaultStorageVersion: decodedConfig.default_storage_version,
      // Right now, only MongoDB source databases supports incremental reprocessing.
      // Remove this filter when we support it for other source databases.
      // This assumes a single source connection - revisit if we ever support multiple connections.
      supportsMultipleSyncConfigs: resolvedConfig.connections?.[0]?.type == lib_mongo.MONGO_CONNECTION_TYPE,

      objectStorage,
      inlineThresholdBytes: decodedConfig.object_storage?.inline_threshold_bytes
    });

    // Storage factory for reports
    const reportStorageFactory = new MongoReportStorage(database);
    return {
      storage: syncStorageFactory,
      reportStorage: reportStorageFactory,
      shutDown: async () => {
        shuttingDown = true;
        await syncStorageFactory[Symbol.asyncDispose]();
        await client.close();
      },
      tearDown: async () => {
        logger.info(`Tearing down storage: ${database.db.namespace}...`);
        if (objectStorage != null) {
          logger.info(
            `Clearing object storage: ${decodedConfig.object_storage!.bucket}/${decodedConfig.object_storage!.prefix ?? ''}.`
          );
          const { objectCount } = await objectStorage.deletePrefix('bucket-data/');
          logger.info(`Deleted ${objectCount} objects from object storage.`);
        }
        return database.db.dropDatabase();
      },
      onFatalError: (callback) => {
        client.addListener('topologyClosed', () => {
          // If we're shutting down, this is expected and we can ignore it.
          if (!shuttingDown) {
            // Unfortunately there is no simple way to catch the cause of this issue.
            // It most commonly happens when the process fails to _ever_ connect - connection issues after
            // the initial connection are usually recoverable.
            callback(
              new ServiceError({
                code: ErrorCode.PSYNC_S2402,
                description: 'MongoDB topology closed - failed to connect to MongoDB storage.'
              })
            );
          }
        });
      }
    } satisfies storage.ActiveStorage;
  }
}
