import * as sqlite from 'node:sqlite';

import {
  HydratedSyncRules,
  SqlSyncRules,
  versionedHydrationState,
  nodeSqliteExpressionEngine,
  addPrecompiledSyncPlanToRules
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors, SyncStreamInput } from '../../compiler/utils.js';
import { test } from 'vitest';
import { ScalarExpressionEngine } from '../../../../src/sync_plan/evaluator/scalar_expression_evaluator.js';

interface SyncTest {
  engine: ScalarExpressionEngine;
  prepareSyncStreams(inputs: SyncStreamInput[]): HydratedSyncRules;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    const engine = nodeSqliteExpressionEngine(sqlite);

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
