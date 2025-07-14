import { BaseEmitterEngine, EmitterEvent, EmitterEventData, EventNames } from './emitter-interfaces.js';
import * as storage from '../storage/storage-index.js';
import { AbstractEmitterEngine } from './AbstractEmitterEngine.js';
import { logger } from '@powersync/lib-services-framework';

export class EmitterEngine extends AbstractEmitterEngine implements BaseEmitterEngine {
  private active: boolean;
  constructor(events: EmitterEvent[], storageRef: storage.StorageEngine) {
    super(storageRef);
    this.active = process.env.MICRO_SERVICE_NAME === 'powersync';
    logger.info(`EmitterEngine initialized with active status: ${this.active}`);
    this.bindEvents(events);
  }

  event(eventName: EventNames): EmitterEvent {
    const event = this.eventsMap.get(eventName);
    if (!event) {
      throw new Error(`Event ${eventName} is not registered.`);
    }
    return event;
  }

  removeListeners(eventName?: EventNames): void {
    if (eventName) {
      this.removeListeners(eventName);
    }
  }
  eventNames(): EventNames[] {
    return this.events;
  }

  emitEvent(eventName: EventNames, data: EmitterEventData): void {
    if (this.active) {
      return this.emit(eventName, data);
    }
  }

  async shutDown(): Promise<void> {
    this.stop();
  }
}
