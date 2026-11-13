import type { storage } from '@powersync/service-core';
import { setTimeout } from 'node:timers/promises';

/** Wait without reading bucket contents. The sync watcher requires an active sync config. */
export async function waitForCheckpointChange(
  bucketStorage: Pick<storage.SyncRulesBucketStorage, 'getCheckpoint' | 'watchCheckpointChanges'>,
  after: string | undefined,
  signal: AbortSignal
): Promise<{ position: string; visibleAtNs: string } | null> {
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal.addEventListener('abort', abort, { once: true });
  try {
    // Before the initial snapshot activates the config, the client checkpoint watcher
    // cannot be opened. Only read checkpoint metadata while waiting for activation.
    while (!signal.aborted) {
      const checkpoint = await bucketStorage.getCheckpoint();
      if (signal.aborted) return null;
      if (checkpoint.lsn != null) {
        if (checkpoint.lsn !== after) {
          return { position: checkpoint.lsn, visibleAtNs: process.hrtime.bigint().toString() };
        }
        break;
      }
      await setTimeout(30, undefined, { signal: controller.signal });
    }
    if (signal.aborted) return null;
    // The watcher reads its initial checkpoint after subscribing, covering updates
    // between the metadata read above and listener registration.
    for await (const update of bucketStorage.watchCheckpointChanges({
      user_id: 'benchmark-checkpoint-wait',
      signal: controller.signal
    })) {
      if (signal.aborted) return null;
      if (update.base.lsn != null && update.base.lsn !== after) {
        return { position: update.base.lsn, visibleAtNs: process.hrtime.bigint().toString() };
      }
    }
    return null;
  } catch (error) {
    if (!signal.aborted) throw error;
    return null;
  } finally {
    controller.abort();
    signal.removeEventListener('abort', abort);
  }
}
