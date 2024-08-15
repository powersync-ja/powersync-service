import { hrtime } from 'node:process';

import * as pgwire from '@powersync/service-jpgwire';
import { Replicator, SyncRulesProvider, DefaultErrorRateLimiter, storage } from '@powersync/service-core';
import { WalStreamRunner } from './WalStreamRunner.js';
import { PgManager } from './PgManager.js';
import { container, logger } from '@powersync/lib-services-framework';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

// 5 minutes
const PING_INTERVAL = 1_000_000_000n * 300n;

export interface WalStreamManagerOptions {
  id: string;
  storageFactory: storage.StorageFactoryProvider;
  syncRuleProvider: SyncRulesProvider;
  connectionFactory: ConnectionManagerFactory;
}

export class WalStreamManager implements Replicator {
  private streams = new Map<number, WalStreamRunner>();
  private connectionManager: PgManager | null = null;

  private stopped = false;

  // First ping is only after 5 minutes, not when starting
  private lastPing = hrtime.bigint();

  /**
   * This limits the effect of retries when there is a persistent issue.
   */
  private rateLimiter = new DefaultErrorRateLimiter();

  constructor(private options: WalStreamManagerOptions) {}

  public get id() {
    return this.options.id;
  }

  private get storage() {
    return this.options.storageFactory.bucketStorage;
  }

  private get syncRuleProvider() {
    return this.options.syncRuleProvider;
  }

  private get connectionFactory() {
    return this.options.connectionFactory;
  }

  start() {
    this.connectionManager = this.connectionFactory.create({ maxSize: 1, idleTimeout: 30000 });
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
    promises.push(this.connectionManager!.end());
    await Promise.all(promises);
  }

  private async runLoop() {
    const configured_sync_rules = await this.syncRuleProvider.get();
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
        const pool = this.connectionManager?.pool;
        if (pool) {
          await this.refresh({ configured_lock });
          // The lock is only valid on the first refresh.
          configured_lock = undefined;

          // TODO: Ping on all connections when we have multiple
          // Perhaps WalStreamRunner would be a better place to do pings?
          // We don't ping while in error retry back-off, to avoid having too
          // many authentication failures.
          if (this.rateLimiter.mayPing()) {
            await this.ping(pool);
          }
        }
      } catch (e) {
        logger.error(`Failed to refresh wal streams`, e);
      }
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  /**
   * Postgres on RDS writes performs a WAL checkpoint every 5 minutes by default, which creates a new 64MB file.
   *
   * The old WAL files are only deleted once no replication slot still references it.
   *
   * Unfortunately, when there are no changes to the db, the database creates new WAL files without the replication slot
   * advancing**.
   *
   * As a workaround, we write a new message every couple of minutes, to make sure that the replication slot advances.
   *
   * **This may be a bug in pgwire or how we're using it.
   */
  private async ping(db: pgwire.PgClient) {
    const now = hrtime.bigint();
    if (now - this.lastPing >= PING_INTERVAL) {
      try {
        await db.query(`SELECT * FROM pg_logical_emit_message(false, 'powersync', 'ping')`);
      } catch (e) {
        logger.warn(`Failed to ping`, e);
      }
      this.lastPing = now;
    }
  }

  private async refresh(options?: { configured_lock?: storage.ReplicationLock }) {
    if (this.stopped) {
      return;
    }

    let configured_lock = options?.configured_lock;

    const existingStreams = new Map<number, WalStreamRunner>(this.streams.entries());
    const replicating = await this.storage.getReplicatingSyncRules();
    const newStreams = new Map<number, WalStreamRunner>();
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
          const stream = new WalStreamRunner({
            factory: this.storage,
            storage: storage,
            connectionFactory: this.connectionFactory,
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
          const stream = new WalStreamRunner({
            factory: this.storage,
            storage: storage,
            connectionFactory: this.connectionFactory,
            lock
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
