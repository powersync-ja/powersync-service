import * as mongo from '../../../db/mongo.js';
import * as storage from '../../../storage/storage-index.js';
import * as utils from '../../../util/util-index.js';

interface LegacySyncRulesDocument extends storage.SyncRuleDocument {
  /**
   * True if this is the active sync rules.
   * requires `snapshot_done == true` and `replicating == true`.
   */
  active?: boolean;

  /**
   * True if this sync rules should be used for replication.
   *
   * During reprocessing, there is one sync rules with `replicating = true, active = true`,
   * and one with `replicating = true, active = false, auto_activate = true`.
   */
  replicating?: boolean;

  /**
   * True if the sync rules should set `active = true` when `snapshot_done` = true.
   */
  auto_activate?: boolean;
}

export const up = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);
  const db = storage.createPowerSyncMongo(config.storage);

  await mongo.waitForAuth(db.db);
  try {
    // We keep the old flags for existing deployments still shutting down.

    // 1. New sync rules: `active = false, snapshot_done = false, replicating = true, auto_activate = true`
    await db.sync_rules.updateMany(
      {
        active: { $ne: true },
        replicating: true,
        auto_activate: true
      },
      { $set: { state: storage.SyncRuleState.PROCESSING } }
    );

    // 2. Snapshot done: `active = true, snapshot_done = true, replicating = true, auto_activate = false`
    await db.sync_rules.updateMany(
      {
        active: true
      },
      { $set: { state: storage.SyncRuleState.ACTIVE } }
    );

    // 3. Stopped: `active = false, snapshot_done = true, replicating = false, auto_activate = false`.
    await db.sync_rules.updateMany(
      {
        active: { $ne: true },
        replicating: { $ne: true },
        auto_activate: { $ne: true }
      },
      { $set: { state: storage.SyncRuleState.STOP } }
    );

    const remaining = await db.sync_rules.find({ state: null as any }).toArray();
    if (remaining.length > 0) {
      const slots = remaining.map((doc) => doc.slot_name).join(', ');
      throw new Error(`Invalid state for sync rules: ${slots}`);
    }
  } finally {
    await db.client.close();
  }
};

export const down = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);

  const db = storage.createPowerSyncMongo(config.storage);
  try {
    await db.sync_rules.updateMany(
      {
        state: storage.SyncRuleState.ACTIVE
      },
      { $set: { active: true, replicating: true } }
    );

    await db.sync_rules.updateMany(
      {
        state: storage.SyncRuleState.PROCESSING
      },
      { $set: { active: false, replicating: true, auto_activate: true } }
    );

    await db.sync_rules.updateMany(
      {
        $or: [{ state: storage.SyncRuleState.STOP }, { state: storage.SyncRuleState.TERMINATED }]
      },
      { $set: { active: false, replicating: false, auto_activate: false } }
    );
  } finally {
    await db.client.close();
  }
};
