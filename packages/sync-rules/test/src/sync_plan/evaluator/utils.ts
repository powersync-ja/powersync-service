import { test } from 'vitest';
import {
  CompatibilityContext,
  CompatibilityEdition,
  CreateSourceParams,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncRules,
  javaScriptExpressionEngine,
  PrecompiledSyncConfig,
  SyncConfig
} from '../../../../src/index.js';
import { ScalarExpressionEngine } from '../../../../src/sync_plan/engine/scalar_expression_engine.js';
import { compileToSyncPlanWithoutErrors, SyncStreamInput } from '../../compiler/utils.js';

interface SyncTest {
  engine: ScalarExpressionEngine;
  prepareWithoutHydration(inputs: SyncStreamInput[]): SyncConfig;
  prepareSyncStreams(inputs: SyncStreamInput[]): HydratedSyncRules;
}

export const syncTest = test.extend<{ sync: SyncTest }>({
  sync: async ({}, use) => {
    const engine = javaScriptExpressionEngine(new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS }));

    await use({
      engine,
      prepareWithoutHydration: (inputs) => {
        const plan = compileToSyncPlanWithoutErrors(inputs);
        return new PrecompiledSyncConfig(plan, { engine, sourceText: '' });
      },
      prepareSyncStreams(inputs, params?: CreateSourceParams) {
        return this.prepareWithoutHydration(inputs).hydrate(params ?? { hydrationState: DEFAULT_HYDRATION_STATE });
      }
    });

    engine.close();
  }
});
