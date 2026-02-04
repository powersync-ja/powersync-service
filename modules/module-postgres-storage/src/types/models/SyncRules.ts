import { framework, storage } from '@powersync/service-core';
import * as t from 'ts-codec';
import { bigint, pgwire_number } from '../codecs.js';

export const SyncRules = t.object({
  id: pgwire_number,
  state: t.Enum(storage.SyncRuleState),
  /**
   * True if initial snapshot has been replicated.
   *
   * Can only be false if state == PROCESSING.
   */
  snapshot_done: t.boolean,
  /**
   * May be set if snapshot_done = false, if the replication stream requires it.
   */
  snapshot_lsn: t.Null.or(t.string),
  /**
   * The last consistent checkpoint.
   *
   * There may be higher OpIds used in the database if we're in the middle of replicating a large transaction.
   */
  last_checkpoint: t.Null.or(bigint),
  /**
   * The LSN associated with the last consistent checkpoint.
   */
  last_checkpoint_lsn: t.Null.or(t.string),
  /**
   * If set, no new checkpoints may be created < this value.
   */
  no_checkpoint_before: t.Null.or(t.string),
  slot_name: t.string,
  /**
   * Last time we persisted a checkpoint.
   *
   * This may be old if no data is incoming.
   */
  last_checkpoint_ts: t.Null.or(framework.codecs.date),
  /**
   * Last time we persisted a checkpoint or keepalive.
   *
   * This should stay fairly current while replicating.
   */
  last_keepalive_ts: t.Null.or(framework.codecs.date),
  /**
   * If an error is stopping replication, it will be stored here.
   */
  last_fatal_error: t.Null.or(t.string),
  keepalive_op: t.Null.or(bigint),
  content: t.string,
  plan: t.Null.or(
    t.object({
      plan: t.any,
      compatibility: t.any
    })
  )
});

export type SyncRules = t.Encoded<typeof SyncRules>;
export type SyncRulesDecoded = t.Decoded<typeof SyncRules>;
