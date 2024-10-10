import * as storage from '../../../storage/storage-index.js';
import * as utils from '../../../util/util-index.js';

const INDEX_NAME = 'user_sync_rule_unique';

export const up = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);
  const db = storage.createPowerSyncMongo(config.storage);

  try {
    await db.custom_write_checkpoints.createIndex(
      {
        user_id: 1,
        sync_rules_id: 1
      },
      { name: INDEX_NAME, unique: true }
    );
  } finally {
    await db.client.close();
  }
};

export const down = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);

  const db = storage.createPowerSyncMongo(config.storage);

  try {
    if (await db.custom_write_checkpoints.indexExists(INDEX_NAME)) {
      await db.custom_write_checkpoints.dropIndex(INDEX_NAME);
    }
  } finally {
    await db.client.close();
  }
};
