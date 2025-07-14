import { EmitterEvent, EmitterEventData, EventConnectData, EventHandler, EventNames } from '../emitter-interfaces.js';
import * as storage from '../../storage/storage-index.js';
import { EventError } from '../event-error.js';
import { logger } from '@powersync/lib-services-framework';

export class ConnectEvent implements EmitterEvent {
  private type = EventNames.SDK_CONNECT_EVENT;
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
      try {
        const disconnectData = data as EventConnectData;
        console.log(
          `Connect event triggered for user: ${disconnectData.user_id} at ${new Date(disconnectData.connect_at).toISOString()}`
        );
      } catch (error) {
        throw new EventError(this.type, error.message);
      }
    };
  }
}

export const connectEvent = new ConnectEvent();
