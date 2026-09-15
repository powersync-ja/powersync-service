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

interface PrepareStreamsOptions {
  allowWarnings: boolean;
}

interface SyncTest {
  prepareWithoutHydration(yaml: string, options?: PrepareStreamsOptions): SyncConfig;
  prepareSyncStreams(
    yaml: string,
    params?: HydrateSyncConfigParams,
    options?: PrepareStreamsOptions
  ): HydratedSyncConfig;
  hydrateConfig(config: SyncConfig, params?: HydrateSyncConfigParams): HydratedSyncConfig;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    await use({
      prepareWithoutHydration: (inputs, options) => {
        const { config, errors } = SqlSyncRules.fromYaml(inputs, { defaultSchema: 'test_schema', throwOnError: false });
        if (!options?.allowWarnings) {
          expect(errors).toStrictEqual([]);
        }

        return config;
      },
      hydrateConfig(config, params?: HydrateSyncConfigParams) {
        return config.hydrate(params ?? { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
      },
      prepareSyncStreams(inputs, params?: HydrateSyncConfigParams, options?: PrepareStreamsOptions) {
        return this.hydrateConfig(this.prepareWithoutHydration(inputs, options), params);
      }
    });
  }
});
