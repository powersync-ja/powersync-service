import {
  HydratedSyncRules,
  versionedHydrationState,
  javaScriptExpressionEngine,
  CompatibilityContext,
  CompatibilityEdition,
  SyncConfig,
  PrecompiledSyncConfig
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors, SyncStreamInput } from '../../compiler/utils.js';
import { test } from 'vitest';
import { ScalarExpressionEngine } from '../../../../src/sync_plan/engine/scalar_expression_engine.js';

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
        return new PrecompiledSyncConfig(plan, { engine, sourceText: '', defaultSchema: 'test_schema' });
      },
      prepareSyncStreams(inputs) {
        return this.prepareWithoutHydration(inputs).hydrate({ hydrationState: versionedHydrationState(1) });
      }
    });

    engine.close();
  }
});
