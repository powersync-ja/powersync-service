import { register } from '@powersync/service-core-tests';
import { updateSyncRulesFromYaml } from '@powersync/service-core';
import { describe, expect, it } from 'vitest';
import type { MikroOrmBucketStorageFactory } from '../../src/index.js';
import { env } from './env.js';
import { MIKRO_ORM_MYSQL_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe.skipIf(!env.MIKROORM_MYSQL_STORAGE_TEST_URI).sequential('MikroORM MySQL Sync Bucket Storage', () => {
  it('creates the schema and stores sync rules', async () => {
    await using factory = (await MIKRO_ORM_MYSQL_STORAGE_FACTORY.factory()) as MikroOrmBucketStorageFactory;
    const stream = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
bucket_definitions:
  global:
    data: []
`,
        {
          validate: false
        }
      )
    );

    await expect(factory.getReplicatingReplicationStreams()).resolves.toMatchObject([
      {
        replicationStreamId: stream.replicationStreamId
      }
    ]);
  });

  for (let storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`Parameters - v${storageVersion}`, () =>
      register.registerDataStorageParameterTests({ ...MIKRO_ORM_MYSQL_STORAGE_FACTORY, storageVersion }));

    describe(`Data - v${storageVersion}`, () =>
      register.registerDataStorageDataTests({ ...MIKRO_ORM_MYSQL_STORAGE_FACTORY, storageVersion }));

    describe(`Checkpoints - v${storageVersion}`, () =>
      register.registerDataStorageCheckpointTests({ ...MIKRO_ORM_MYSQL_STORAGE_FACTORY, storageVersion }));

    describe(`Compaction - v${storageVersion}`, () => {
      register.registerCompactTests({ ...MIKRO_ORM_MYSQL_STORAGE_FACTORY, storageVersion });
      register.registerParameterCompactTests({ ...MIKRO_ORM_MYSQL_STORAGE_FACTORY, storageVersion });
    });
  }
});
