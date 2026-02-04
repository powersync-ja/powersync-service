import {
  HydratedSyncRules,
  SqlSyncRules,
  versionedHydrationState,
  addPrecompiledSyncPlanToRules,
  javaScriptExpressionEngine,
  CompatibilityContext,
  CompatibilityEdition,
  DEFAULT_HYDRATION_STATE,
  CreateSourceParams
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors, SyncStreamInput } from '../../compiler/utils.js';
import { test } from 'vitest';
import { ScalarExpressionEngine } from '../../../../src/sync_plan/engine/scalar_expression_engine.js';

interface SyncTest {
  engine: ScalarExpressionEngine;
  prepareWithoutHydration(inputs: SyncStreamInput[]): SqlSyncRules;
  prepareSyncStreams(inputs: SyncStreamInput[]): HydratedSyncRules;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    const engine = javaScriptExpressionEngine(new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS }));

    await use({
      engine,
      prepareWithoutHydration: (inputs) => {
        const plan = compileToSyncPlanWithoutErrors(inputs);
        const rules = new SqlSyncRules('');

        addPrecompiledSyncPlanToRules(plan, rules, { engine });
        return rules;
      },
      prepareSyncStreams(inputs, params?: CreateSourceParams) {
        return this.prepareWithoutHydration(inputs).hydrate(params ?? { hydrationState: DEFAULT_HYDRATION_STATE });
      }
    });

    engine.close();
  }
});
