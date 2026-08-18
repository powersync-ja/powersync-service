import { defineEntity, p, type InferEntity } from '@mikro-orm/core';
import type { storage } from '@powersync/service-core';

export const SyncRulesSchema = defineEntity({
  name: 'SyncRules',
  tableName: 'sync_rules',
  properties: {
    id: p.integer().primary().autoincrement(),
    state: p.string().$type<storage.SyncRuleState>(),
    snapshotDone: p.boolean().fieldName('snapshot_done').default(false),
    snapshotLsn: p.string().fieldName('snapshot_lsn').nullable(),
    lastCheckpoint: p.bigint('bigint').nullable(),
    lastCheckpointLsn: p.string().fieldName('last_checkpoint_lsn').strictNullable(),
    noCheckpointBefore: p.string().fieldName('no_checkpoint_before').nullable(),
    slotName: p.string().fieldName('slot_name'),
    lastCheckpointTs: p.datetime().nullable(),
    lastKeepaliveTs: p.datetime().nullable(),
    lastFatalError: p.string().fieldName('last_fatal_error').strictNullable(),
    lastFatalErrorTs: p.datetime().strictNullable(),
    keepaliveOp: p.bigint('bigint').nullable(),
    storageVersion: p.integer().fieldName('storage_version').nullable(),
    content: p.text(),
    syncPlan: p.json<storage.SerializedSyncPlan>().strictNullable()
  }
});

export const SyncRules = SyncRulesSchema.class;
export type SyncRules = InferEntity<typeof SyncRulesSchema>;
