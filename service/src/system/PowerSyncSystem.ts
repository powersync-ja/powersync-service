import { db, system, utils, storage, Metrics } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';

export class PowerSyncSystem extends system.CorePowerSyncSystem {
  storage: storage.BucketStorageFactory;
  pgwire_pool?: pgwire.PgClient;

  constructor(public config: utils.ResolvedPowerSyncConfig) {
    super(config);

    utils.setTags(config.metadata);

    const pgOptions = config.connection;
    if (pgOptions != null) {
      const pool = pgwire.connectPgWirePool(pgOptions, {
        idleTimeout: 30_000
      });
      this.pgwire_pool = this.withLifecycle(pool, {
        async start(pool) {},
        async stop(pool) {
          await pool.end();
        }
      });
    }

    if (config.storage.type == 'mongodb') {
      const client = this.withLifecycle(db.mongo.createMongoClient(config.storage), {
        async start(client) {},
        async stop(client) {
          await client.close();
        }
      });
      const database = new storage.PowerSyncMongo(client, { database: config.storage.database });
      this.storage = new storage.MongoBucketStorage(database, {
        slot_name_prefix: config.slot_name_prefix
      });
    } else {
      throw new Error('No storage configured');
    }

    this.withLifecycle(this.storage, {
      async start(storage) {
        const instanceId = await storage.getPowerSyncInstanceId();
        await Metrics.initialise({
          powersync_instance_id: instanceId,
          disable_telemetry_sharing: config.telemetry.disable_telemetry_sharing,
          internal_metrics_endpoint: config.telemetry.internal_service_endpoint
        });
      },
      async stop() {
        await Metrics.getInstance().shutdown();
      }
    });
  }
}
