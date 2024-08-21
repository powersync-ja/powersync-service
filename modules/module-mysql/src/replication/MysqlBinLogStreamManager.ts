import { logger } from '@powersync/lib-services-framework';
import { storage, system, utils } from '@powersync/service-core';
import mysql from 'mysql2/promise';
import { hrtime } from 'node:process';
import { normalizeConnectionConfig } from '../types/types.js';
import { MysqlBinLogStreamRunner } from './MysqlBinLogStreamRunner.js';

// 5 minutes
const PING_INTERVAL = 1_000_000_000n * 300n;

export class MysqlBinLogStreamManager {
  private streams = new Map<number, MysqlBinLogStreamRunner>();

  private stopped = false;

  // First ping is only after 5 minutes, not when starting
  private lastPing = hrtime.bigint();

  private storage: storage.BucketStorageFactory;

  constructor(protected serviceContext: system.ServiceContext, protected pool: mysql.Pool) {
    this.storage = serviceContext.storage.bucketStorage;
  }

  start() {
    this.runLoop().catch((e) => {
      console.error(e);
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
    const configured_sync_rules = await utils.loadSyncRules(this.serviceContext.configuration);
    let configured_lock: storage.ReplicationLock | undefined = undefined;
    if (configured_sync_rules != null) {
      logger.info('Loading sync rules from configuration');
      try {
        // Configure new sync rules, if it has changed.
        // In that case, also immediately take out a lock, so that another process doesn't start replication on it.
        const { updated, persisted_sync_rules, lock } = await this.storage.configureSyncRules(configured_sync_rules!, {
          lock: true
        });
        logger.info(`configureSyncRules: update: ${updated}, persisted_sync_rules: ${persisted_sync_rules}`);
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
      // await touch();
      try {
        const { pool } = this;
        if (pool) {
          await this.refresh({ configured_lock });
          // The lock is only valid on the first refresh.
          configured_lock = undefined;

          // TODO: Ping on all connections when we have multiple
          // Perhaps MysqlWalStreamRunner would be a better place to do pings?
          // We don't ping while in error retry back-off, to avoid having too
          // many authentication failures.
          // if (this.rateLimiter.mayPing()) {
          // await this.ping(pool);
          // }
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
  private async ping(db: mysql.Pool) {
    const now = hrtime.bigint();
    if (now - this.lastPing >= PING_INTERVAL) {
      try {
        await db.query('SELECT 1');
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

    const existingStreams = new Map<number, MysqlBinLogStreamRunner>(this.streams.entries());
    const replicating = await this.storage.getReplicatingSyncRules();
    const newStreams = new Map<number, MysqlBinLogStreamRunner>();
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

          const stream = new MysqlBinLogStreamRunner({
            connection_config: normalizeConnectionConfig(this.serviceContext.configuration.connections?.[0] as any), // TODO
            factory: this.storage,
            storage: storage,
            source_db: this.pool,
            lock
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

    for (let stream of existingStreams.values()) {
      // Old - stop and remove.
      await stream.terminate();
    }

    // Sync rules stopped previously or by a different process.
    const stopped = await this.storage.getStoppedSyncRules();
    for (let syncRules of stopped) {
      try {
        const lock = await syncRules.lock();
        try {
          const parsed = syncRules.parsed();
          const storage = this.storage.getInstance(parsed);
          const stream = new MysqlBinLogStreamRunner({
            connection_config: normalizeConnectionConfig(this.serviceContext.configuration.connections?.[0] as any), // TODO
            factory: this.storage,
            storage: storage,
            source_db: this.pool,
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
