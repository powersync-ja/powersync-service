import {
  EmitterEvent,
  EmitterEventData,
  EventDisconnectData,
  EventHandler,
  EventNames
} from '../emitter-interfaces.js';
import * as storage from '../../storage/storage-index.js';
import { EventError } from '../event-error.js';
import { logger } from '@powersync/lib-services-framework';

export class DisconnectEvent implements EmitterEvent {
  private type = EventNames.SDK_DISCONNECT_EVENT;
  get name(): EventNames {
    return this.type;
  }
  errorhandler(error: Error | EventError): void {
    if (error instanceof EventError) {
      logger.error(`EventError in ${error.eventName}:`, error.message);
    } else {
      logger.error(error.message);
    }
  }
  handler(storageEngine: storage.StorageEngine): EventHandler {
    // TODO: USE STORAGE ENGINE
    const storage = storageEngine.activeStorage.storage;
    return async (data: EmitterEventData) => {
      const disconnectData = data as EventDisconnectData;
      console.log(
        `Disconnect event triggered for user: ${disconnectData.user_id} at ${new Date(disconnectData.disconnect_at).toISOString()}`
      );
    };
  }
}

export const disconnectEvent = new DisconnectEvent();
