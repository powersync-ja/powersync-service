import * as sqlite from 'node:sqlite';

import { expect, test } from 'vitest';
import {
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  HydrateSyncConfigParams,
  nodeSqlite,
  SqlSyncRules,
  SyncConfig
} from '../../../../src/index.js';

interface SyncTest {
  prepareWithoutHydration(yaml: string): SyncConfig;
  prepareSyncStreams(yaml: string, params?: HydrateSyncConfigParams): HydratedSyncConfig;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    await use({
      prepareWithoutHydration: (inputs) => {
        const { config, errors } = SqlSyncRules.fromYaml(inputs, { defaultSchema: 'test_schema', throwOnError: false });
        expect(errors).toStrictEqual([]);

        return config;
      },
      prepareSyncStreams(inputs, params?: HydrateSyncConfigParams) {
        return this.prepareWithoutHydration(inputs).hydrate(
          params ?? { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }
        );
      }
    });
  }
});
