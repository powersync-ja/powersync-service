import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';

/**
 * Update pipeline to stop a deploying replication stream, covering all storage versions.
 *
 * Roughly equivalent to:
 *   $set: {
 *     state: STOP,
 *     'sync_configs.$[].state': STOP
 *   }
 *
 * The difference is that this also handles v1 storage cases, where `sync_configs` is not present.
 *
 * To use this, the update operatoin MUST use a top-level { state: PROCESSING } filter. This is not
 * safe to use when the replication stream may be ACTIVE.
 */
export function stopProcessingSyncRuleStateUpdatePipeline(): mongo.Document[] {
  return [
    {
      $set: {
        state: storage.SyncRuleState.STOP,
        sync_configs: {
          $cond: [
            { $isArray: '$sync_configs' },
            {
              $map: {
                input: '$sync_configs',
                as: 'config',
                in: {
                  $mergeObjects: ['$$config', { state: storage.SyncRuleState.STOP }]
                }
              }
            },
            '$$REMOVE'
          ]
        }
      }
    }
  ];
}
