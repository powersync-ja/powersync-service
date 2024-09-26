import { logger } from '@powersync/lib-services-framework';
import * as sync_rules from '@powersync/service-sync-rules';
import { BucketStorageBatch, SaveOp } from './BucketStorage.js';
import { SourceTable } from './SourceTable.js';

export type EventData = {
  op: SaveOp;
  before?: sync_rules.SqliteRow;
  after?: sync_rules.SqliteRow;
};

export type ReplicationEventPayload = {
  batch: BucketStorageBatch;
  data: EventData;
  event: sync_rules.SqlEventDescriptor;
  table: SourceTable;
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

  /**
   * Fires an event, passing the specified payload to all registered handlers.
   * This call resolves once all handlers have processed the event.
   * Handler exceptions are caught and logged.
   */
  async fireEvent(payload: ReplicationEventPayload): Promise<void> {
    const handlers = this.handlers.get(payload.event.name);

    for (const handler of handlers?.values() ?? []) {
      try {
        await handler.handle(payload);
      } catch (ex) {
        // Exceptions in handlers don't affect the source.
        logger.info(`Caught exception when processing "${handler.event_name}" event.`, ex);
      }
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
