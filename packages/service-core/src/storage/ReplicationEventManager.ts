import * as sync_rules from '@powersync/service-sync-rules';
import { SaveOp, SyncRulesBucketStorage } from './BucketStorage.js';
import { SourceTable } from './SourceTable.js';

export type EventData = {
  op: SaveOp;
  before?: sync_rules.SqliteRow;
  after?: sync_rules.SqliteRow;
};

export type ReplicationEventData = Map<SourceTable, EventData[]>;

export type ReplicationEventPayload = {
  event: sync_rules.SqlEventDescriptor;
  data: ReplicationEventData;
  storage: SyncRulesBucketStorage;
};

export interface ReplicationEventHandler {
  event_name: string;
  handle(event: ReplicationEventPayload): Promise<void>;
}

export class ReplicationEventManager {
  handlers: Map<string, Set<ReplicationEventHandler>>;

  constructor() {
    this.handlers = new Map();
  }

  async fireEvent(payload: ReplicationEventPayload): Promise<void> {
    const handlers = this.handlers.get(payload.event.name);

    for (const handler of handlers?.values() ?? []) {
      await handler.handle(payload);
    }
  }

  registerHandler(handler: ReplicationEventHandler) {
    const { event_name } = handler;
    if (!this.handlers.has(event_name)) {
      this.handlers.set(event_name, new Set());
    }
    this.handlers.get(event_name)?.add(handler);
  }
}
