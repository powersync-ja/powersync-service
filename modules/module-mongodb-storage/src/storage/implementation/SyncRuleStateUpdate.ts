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
 * This can be used for specific use cases:
 * 1. Stop an existing PROCESSING stream, when it is being replaced by a new PROCESSING one.
 * 2. Stop an existing ACTIVE or ERRORED stream, when it is being replaced by a new ACTIVE one.
 *
 * Do not use this when an ACTIVE stream transitions to ERRORED, and is being replaced by a new PROCESSING one.
 * In that case, we need to maintain the ERRORED state, which this pipeline does not cover.
 */
export function stopReplicationStreamPipeline(): mongo.Document[] {
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
