import * as sync_rules from '@powersync/service-sync-rules';
import { BucketStorageBatch, SaveOp } from './BucketStorageBatch.js';
import { SourceTable } from './SourceTable.js';

export type EventData = {
  op: SaveOp;
  before?: sync_rules.SqliteInputRow;
  after?: sync_rules.SqliteInputRow;
};

export type ReplicationEventPayload = {
  batch: BucketStorageBatch;
  data: EventData;
  event: sync_rules.HydratedEventDescriptor;
  /**
   * Storage-assigned id for this event definition. Present when storage uses persisted event mappings, allowing event
   * handlers to route custom checkpoints without deriving an id from compiled event content.
   */
  event_id?: sync_rules.EventDefinitionId;
  table: SourceTable;
};
