import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, register } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { models } from '../../src/types/types.js';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe.each(TEST_STORAGE_VERSIONS)('invalid sync config, storage v%i', (storageVersion) => {
  register.registerInvalidSyncConfigTests({ ...POSTGRES_STORAGE_FACTORY, storageVersion });

  /**
   * Deploy invalid A and then another invalid attempt B, which stops A. Terminate and clear
   * A: its stored YAML and validation errors must survive, while diagnostics selects B.
   */
  test('retains invalid content and errors after stopped storage is cleared', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY.factory();
    const options = updateSyncRulesFromYaml(
      /* yaml */ `
        # Invalid deployment retained for history.
        config:
          edition: 3
        streams:
          broken:
            query: SELECT FROM
      `,
      { storageVersion, validate: false }
    );
    const invalid = await factory.updateSyncRules(options);
    const replacement = await factory.updateSyncRules(options);
    expect((await invalid.syncConfigContent[0].getSyncConfigStatus())?.state).toBe(storage.SyncRuleState.STOP);
    const stopped = (await factory.getStoppedReplicationStreams())[0];
    const bucketStorage = await getTestStorage(factory, stopped);
    await bucketStorage.terminate({ clearStorage: true });
    const history = await factory.db.sql`
      SELECT * FROM sync_rules WHERE id = ${{ type: 'int4', value: invalid.replicationStreamId }}
    `
      .decoded(models.SyncRules)
      .first();
    expect(history?.content).toBe(options.config.yaml);
    expect(history?.sync_plan).toEqual(options.config.plan);
    expect((await factory.getSyncConfigStatus()).next?.content.replicationStreamId).toBe(
      replacement.replicationStreamId
    );
  });
});
