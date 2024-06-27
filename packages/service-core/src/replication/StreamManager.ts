import { hrtime } from 'node:process';

import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';
import { container, logger } from '@powersync/lib-services-framework';
import { DefaultErrorRateLimiter, ErrorRateLimiter } from './ErrorRateLimiter.js';
import { AbstractStreamRunner, StreamRunnerOptions } from './StreamRunner.js';

export interface StreamRunnerFactory<ConnectionConfig extends {} = {}> {
  readonly type: string;
  generate(options: StreamRunnerOptions<ConnectionConfig>): Promise<AbstractStreamRunner>;
}

// 5 minutes
const PING_INTERVAL = 1_000_000_000n * 300n;

export class WalStreamManager {
  private streams: Map<number, AbstractStreamRunner>;
  private runnerFactories: Map<string, StreamRunnerFactory>;

  private stopped: boolean;

  // First ping is only after 5 minutes, not when starting
  private lastPing: bigint;

  private storage: storage.BucketStorageFactory;

  /**
   * This limits the effect of retries when there is a persistent issue.
   */
  private rateLimiter: ErrorRateLimiter;

  constructor(protected system: CorePowerSyncSystem) {
    this.storage = system.storage;
    this.rateLimiter = new DefaultErrorRateLimiter();
    this.streams = new Map();
    this.runnerFactories = new Map();
    this.stopped = false;
    this.lastPing = hrtime.bigint();
  }

  registerRunnerFactory(factory: StreamRunnerFactory) {
    this.runnerFactories.set(factory.type, factory);
  }

  start() {
    this.runLoop().catch((e) => {
      logger.error(`Fatal WalStream error`, e);
      container.reporter.captureException(e);
      setTimeout(() => {
        process.exit(1);
      }, 1000);
    });
  }

  async stop() {
    this.stopped = true;
    let promises: Promise<void>[] = [];
    for (let stream of this.streams.values()) {
      promises.push(stream.stop());
    }
    await Promise.all(promises);
  }

  private async runLoop() {
    const configured_sync_rules = await util.loadSyncRules(this.system.config);
    let configured_lock: storage.ReplicationLock | undefined = undefined;
    if (configured_sync_rules != null) {
      logger.info('Loading sync rules from configuration');
      try {
        // Configure new sync rules, if it has changed.
        // In that case, also immediately take out a lock, so that another process doesn't start replication on it.
        const { updated, persisted_sync_rules, lock } = await this.storage.configureSyncRules(configured_sync_rules!, {
          lock: true
        });
        if (lock) {
          configured_lock = lock;
        }
      } catch (e) {
        // Log, but continue with previous sync rules
        logger.error(`Failed to load sync rules from configuration`, e);
      }
    } else {
      logger.info('No sync rules configured - configure via API');
    }
    while (!this.stopped) {
      await container.probes.touch();
      try {
        await this.refresh({ configured_lock });
        // The lock is only valid on the first refresh.
        configured_lock = undefined;
      } catch (e) {
        logger.error(`Failed to refresh wal streams`, e);
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  private async refresh(options?: { configured_lock?: storage.ReplicationLock }) {
    if (this.stopped) {
      return;
    }

    let configured_lock = options?.configured_lock;

    const existingStreams = new Map<number, AbstractStreamRunner>(this.streams.entries());
    const replicating = await this.storage.getReplicatingSyncRules();
    const newStreams = new Map<number, AbstractStreamRunner>();
    for (let syncRules of replicating) {
      const existing = existingStreams.get(syncRules.id);
      if (existing && !existing.stopped) {
        // No change
        existingStreams.delete(syncRules.id);
        newStreams.set(syncRules.id, existing);
      } else if (existing && existing.stopped) {
        // Stopped (e.g. fatal error, slot rename).
        // Remove from the list. Next refresh call will restart the stream.
        existingStreams.delete(syncRules.id);
      } else {
        // New (or resume after restart)
        try {
          let lock: storage.ReplicationLock;
          if (configured_lock?.sync_rules_id == syncRules.id) {
            lock = configured_lock;
          } else {
            lock = await syncRules.lock();
          }
          const parsed = syncRules.parsed();
          const storage = this.storage.getInstance(parsed);
          // TODO multiple connections
          const {
            config: { connection }
          } = this.system;
          const { type } = connection!;
          const factory = this.runnerFactories.get(type);
          if (!factory) {
            throw new Error(`No replication implementation for connection type: ${type}`);
          }
          const stream = await factory.generate({
            config: connection!,
            factory: this.storage,
            storage,
            lock,
            rateLimiter: this.rateLimiter
          });

          newStreams.set(syncRules.id, stream);
          stream.start();
        } catch (e) {
          // Could be a sync rules parse error,
          // for example from stricter validation that was added.
          // This will be retried every couple of seconds.
          // When new (valid) sync rules are deployed and processed, this one be disabled.
          logger.error(`Failed to start replication for ${syncRules.slot_name}`, e);
        }
      }
    }

    this.streams = newStreams;

    // TODO: Should this termination be happening in the "background" instead?
    // That becomes tricky to manage

    for (let stream of existingStreams.values()) {
      // Old - stop and remove.
      try {
        await stream.terminate();
      } catch (e) {
        // This will be retried
        logger.warn(`Failed to terminate ${stream.slot_name}`, e);
      }
    }

    // Sync rules stopped previously or by a different process.
    const stopped = await this.storage.getStoppedSyncRules();
    for (let syncRules of stopped) {
      try {
        const lock = await syncRules.lock();
        try {
          const parsed = syncRules.parsed();
          const storage = this.storage.getInstance(parsed);
          const {
            config: { connection }
          } = this.system;
          const { type } = connection!;
          const factory = this.runnerFactories.get(type);
          if (!factory) {
            throw new Error(`No replication implementation for connection type: ${type}`);
          }
          const stream = await factory.generate({
            config: connection!,
            factory: this.storage,
            storage,
            lock,
            rateLimiter: this.rateLimiter
          });
          await stream.terminate();
        } finally {
          await lock.release();
        }
      } catch (e) {
        logger.warn(`Failed to terminate ${syncRules.slot_name}`, e);
      }
    }
  }
}
