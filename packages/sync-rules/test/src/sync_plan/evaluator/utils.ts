import * as sqlite from 'node:sqlite';

import { test } from 'vitest';
import {
  CompatibilityContext,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncConfig,
  HydrateSyncConfigParams,
  nodeSqlite,
  PrecompiledSyncConfig,
  SyncConfig
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors } from '../../compiler/utils.js';

interface SyncTest {
  prepareWithoutHydration(yaml: string): SyncConfig;
  prepareSyncStreams(yaml: string, params?: HydrateSyncConfigParams): HydratedSyncConfig;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    await use({
      prepareWithoutHydration: (inputs) => {
        const plan = compileToSyncPlanWithoutErrors(inputs);
        return new PrecompiledSyncConfig(plan, new CompatibilityContext({ edition: 3 }), [], {
          sourceText: '',
          defaultSchema: 'test_schema'
        });
      },
      prepareSyncStreams(inputs, params?: HydrateSyncConfigParams) {
        return this.prepareWithoutHydration(inputs).hydrate(
          params ?? { hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) }
        );
      }
    });
  }
});
