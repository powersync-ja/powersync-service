import * as sqlite from 'node:sqlite';

import {
  addPrecompiledSyncPlanToRules,
  HydratedSyncRules,
  nodeSqlEngine,
  SqlSyncRules,
  versionedHydrationState
} from '../../../src/index.js';
import { compileToSyncPlanWithoutErrors, SyncStreamInput } from '../compiler/utils.js';
import { test } from 'vitest';
import { SqlEngine } from '../../../src/sync_plan/sql_engine.js';

interface SyncTest {
  engine: SqlEngine;
  prepareSyncStreams(inputs: SyncStreamInput[]): HydratedSyncRules;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    const engine = nodeSqlEngine(sqlite);

    await use({
      engine,
      prepareSyncStreams: (inputs) => {
        const plan = compileToSyncPlanWithoutErrors(inputs);
        const rules = new SqlSyncRules('');

        addPrecompiledSyncPlanToRules(plan, rules, { engine });
        return rules.hydrate({ hydrationState: versionedHydrationState(1) });
      }
    });

    engine.close();
  }
});
