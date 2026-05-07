import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';

/**
 * Update pipeline to update replication stream status, covering all storage versions.
 *
 * Roughly equivalent to:
 *   $set: {
 *     state: state,
 *     'sync_configs.$[].state': state
 *   }
 *
 * The difference is that this also handles v1 storage cases, where `sync_configs` is not present.
 */
export function syncRuleStateUpdatePipeline(state: storage.SyncRuleState): mongo.Document[] {
  return [
    {
      $set: {
        state,
        sync_configs: {
          $cond: [
            { $isArray: '$sync_configs' },
            {
              $map: {
                input: '$sync_configs',
                as: 'config',
                in: {
                  $mergeObjects: ['$$config', { state }]
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
