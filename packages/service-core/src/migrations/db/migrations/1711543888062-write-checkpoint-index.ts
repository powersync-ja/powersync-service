import * as storage from '../../../storage/storage-index.js';
import * as utils from '../../../util/util-index.js';

export const up = async (context: utils.MigrationContext) => {
  const { service_context } = context;
  const db = storage.createPowerSyncMongo(service_context.configuration.storage);

  try {
    await db.write_checkpoints.createIndex(
      {
        user_id: 1
      },
      { name: 'user_id' }
    );
  } finally {
    await db.client.close();
  }
};

export const down = async (context: utils.MigrationContext) => {
  const { service_context } = context;

  const db = storage.createPowerSyncMongo(service_context.configuration.storage);

  try {
    if (await db.write_checkpoints.indexExists('user_id')) {
      await db.write_checkpoints.dropIndex('user_id');
    }
  } finally {
    await db.client.close();
  }
};
