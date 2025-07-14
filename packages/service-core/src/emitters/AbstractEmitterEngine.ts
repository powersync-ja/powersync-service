import { EmitterEvent, EmitterEventData, EventNames, isEventError } from './emitter-interfaces.js';
import * as storage from '../storage/storage-index.js';
import EventEmitter from 'node:events';
import { logger } from '@powersync/lib-services-framework';
import { EventError } from './event-error.js';

export abstract class AbstractEmitterEngine {
  private emitter: EventEmitter;
  storageEngine: storage.StorageEngine;
  eventsMap: Map<EventNames, EmitterEvent> = new Map();

  protected constructor(storage: storage.StorageEngine) {
    this.emitter = new EventEmitter({ captureRejections: true });
    this.storageEngine = storage;
    this.emitter.on('error', (error: Error | EventError) => {
      if (isEventError(error) && this.eventsMap.has(error.eventName)) {
        const event = this.eventsMap.get(error.eventName)!;
        const errorHandler =
          event.errorhandler ??
          ((error) => {
            logger.error(error.message);
          });
        errorHandler(error);
      }
    });
  }

  get events(): EventNames[] {
    return this.emitter.eventNames() as EventNames[];
  }

  protected bindEvents(events: EmitterEvent[]): void {
    const eventNames = this.emitter.eventNames();
    for (const event of events) {
      if (!eventNames.includes(event.name)) {
        this.eventsMap.set(event.name, event);
        this.emitter.on(event.name, event.handler(this.storageEngine).bind(event));
      } else {
        logger.warn(`Event ${event.name} is already registered. Skipping.`);
      }
    }
  }

  protected emit(eventName: EventNames, data: EmitterEventData): void {
    if (!this.emitter.eventNames().includes(eventName)) {
      throw new Error(`Event ${eventName} is not registered.`);
    }
    this.emitter.emit(eventName, data);
  }

  protected removeListeners(eventName?: EventNames): void {
    if (eventName) {
      this.emitter.removeAllListeners(eventName);
      logger.info(`Removed all listeners for event: ${eventName}`);
    } else {
      this.stop();
    }
  }

  protected stop(): void {
    this.emitter.removeAllListeners();
    logger.info('Emitter engine shut down and all listeners removed.');
  }
}
