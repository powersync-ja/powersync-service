import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { register, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test, vi } from 'vitest';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`storage v${storageVersion}`, () => {
    register.registerInvalidSyncConfigTests(POSTGRES_STORAGE_FACTORY, storageVersion);

    test.each([{ deploymentTiming: 'before recovery reads' }, { deploymentTiming: 'after recovery reads' }] as const)(
      'preserves a deployment submitted after cancellation: $deploymentTiming',
      async ({ deploymentTiming }) => {
        await using backend = await POSTGRES_STORAGE_FACTORY.factory();
        await using otherWorker = await POSTGRES_STORAGE_FACTORY.factory({ doNotClear: true });
        const content = /* yaml */ `
          # Existing active deployment.
          config:
            edition: 3
          streams:
            todos:
              query: SELECT id FROM todos
        `;
        const original = await backend.updateSyncRules(
          updateSyncRulesFromYaml(content, { storageVersion, version_label: 'active' })
        );
        const instance = await test_utils.getTestStorage(backend, original);
        {
          await using writer = await instance.createWriter(test_utils.BATCH_OPTIONS);
          await writer.markAllSnapshotDone('1/1');
          await writer.commit('1/1');
        }
        await backend.restartReplication(original.replicationStreamId);
        // Model rejected startup cancelling recovery while A remains errored and readable.
        await backend.stopProcessingSyncConfigs();
        expect((await backend.getActiveSyncConfig())?.replicationStream.state).toBe(storage.SyncRuleState.ERRORED);

        const replacement = updateSyncRulesFromYaml(content + '\n# New valid deployment C', {
          storageVersion,
          version_label: 'newer'
        });
        const transaction = backend.db.transaction.bind(backend.db);
        const recoveryRead = Promise.withResolvers<void>();
        const resumeRecovery = Promise.withResolvers<void>();
        using race = vi.spyOn(backend.db, 'transaction').mockImplementationOnce((callback) =>
          transaction(async (db) => {
            if (deploymentTiming === 'before recovery reads') {
              // Commit C at the former gap between recovery's read and replacement transaction.
              await otherWorker.updateSyncRules(replacement);
            } else {
              const queryRows = db.queryRows.bind(db);
              // Pause after the recovery snapshot is read, keeping its transaction open.
              vi.spyOn(db, 'queryRows').mockImplementationOnce(async function <T>(
                ...args: Parameters<typeof db.queryRows>
              ): Promise<T[]> {
                const rows = await queryRows<T>(...args);
                recoveryRead.resolve();
                await resumeRecovery.promise;
                return rows;
              });
            }
            return callback(db);
          })
        );
        const recovering = backend.restartReplication(original.replicationStreamId);
        if (deploymentTiming === 'before recovery reads') {
          await recovering;
        } else {
          await recoveryRead.promise;
          let deploymentFinished = false;
          const deploying = otherWorker.updateSyncRules(replacement).finally(() => {
            deploymentFinished = true;
          });
          try {
            // Wait until C either commits or is blocked by recovery's metadata lock. Without the
            // lock it commits first, and recovery then incorrectly cancels it using its stale read.
            await expect
              .poll(async () => {
                const [waiting] = await backend.db.sql`
              SELECT EXISTS (
                SELECT 1 FROM pg_locks
                WHERE relation = 'powersync.sync_rules'::regclass AND NOT granted
              ) AS blocked
            `.rows<{ blocked: boolean }>();
                return deploymentFinished || waiting.blocked;
              })
              .toBe(true);
          } finally {
            resumeRecovery.resolve();
            await Promise.all([recovering, deploying]);
          }
        }

        const pending = await backend.getDeployingSyncConfig();
        expect(pending?.content.sync_rules_content).toBe(replacement.config.yaml);
        expect(pending?.content.version_label).toBe('newer');
        expect((await backend.getActiveSyncConfig())?.content.sync_rules_content).toBe(content);
      }
    );
  });
}
