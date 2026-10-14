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
 *
 * With preserveInvalid=true, stop the stream but retain its embedded INVALID config states for
 * diagnostics. Activation uses this because the replacement may be automatic recovery, which
 * does not supersede a rejected operator deployment. A new operator deployment stops superseded
 * invalid attempts when it is persisted, before activation.
 * With preserveInvalid=false (the default), stop every embedded config as well as the stream.
 */
export function stopReplicationStreamPipeline(options: { preserveInvalid?: boolean } = {}): mongo.Document[] {
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
                  $mergeObjects: [
                    '$$config',
                    {
                      state: options.preserveInvalid
                        ? {
                            $cond: [
                              { $eq: ['$$config.state', storage.SyncRuleState.INVALID] },
                              storage.SyncRuleState.INVALID,
                              storage.SyncRuleState.STOP
                            ]
                          }
                        : storage.SyncRuleState.STOP
                    }
                  ]
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
