import type { Opt } from '@mikro-orm/core';
import { defineEntity, p } from '@mikro-orm/core';
import type { storage } from '@powersync/service-core';

export class SyncRules {
  declare id: Opt<number>;
  declare state: storage.SyncRuleState;
  declare snapshotDone: Opt<boolean>;
  declare snapshotLsn: string | null;
  declare lastCheckpoint: bigint | null;
  declare lastCheckpointLsn: string | null;
  declare noCheckpointBefore: string | null;
  declare slotName: string;
  declare lastCheckpointTs: Date | null;
  declare lastKeepaliveTs: Date | null;
  declare lastFatalError: string | null;
  declare lastFatalErrorTs: Date | null;
  declare keepaliveOp: bigint | null;
  declare storageVersion: number | null;
  declare content: string;
  declare syncPlan: storage.SerializedSyncPlan | null;
}

export const SyncRulesSchema = defineEntity({
  name: 'SyncRules',
  tableName: 'sync_rules',
  properties: {
    id: p.integer().primary().autoincrement(),
    state: p.string(),
    snapshotDone: p.boolean().fieldName('snapshot_done').default(false),
    snapshotLsn: p.string().fieldName('snapshot_lsn').nullable(),
    lastCheckpoint: p.bigint('bigint').nullable(),
    lastCheckpointLsn: p.string().fieldName('last_checkpoint_lsn').nullable(),
    noCheckpointBefore: p.string().fieldName('no_checkpoint_before').nullable(),
    slotName: p.string().fieldName('slot_name'),
    lastCheckpointTs: p.datetime().nullable(),
    lastKeepaliveTs: p.datetime().nullable(),
    lastFatalError: p.string().fieldName('last_fatal_error').nullable(),
    lastFatalErrorTs: p.datetime().nullable(),
    keepaliveOp: p.bigint('bigint').nullable(),
    storageVersion: p.integer().fieldName('storage_version').nullable(),
    content: p.string(),
    syncPlan: p.json<storage.SerializedSyncPlan>().nullable()
  }
});
SyncRulesSchema.setClass(SyncRules);
