import { BaseEmitterEngine } from './emitter-interfaces.js';
import EventEmitter from 'node:events';
import { logger } from '@powersync/lib-services-framework';
import { event_types } from '@powersync/service-types';

export class EmitterEngine implements BaseEmitterEngine {
  private emitter: EventEmitter;
  eventsMap: Map<event_types.EmitterEngineEventNames, event_types.EmitterEvent> = new Map();
  constructor() {
    this.emitter = new EventEmitter({ captureRejections: true });
    this.emitter.on('error', (error: Error) => {
      logger.error(error);
    });
  }

  eventNames(): event_types.EmitterEngineEventNames[] {
    return this.emitter.eventNames() as event_types.EmitterEngineEventNames[];
  }

  getStoredEvent(eventName: event_types.EmitterEngineEventNames): event_types.EmitterEvent {
    if (!this.eventsMap.has(eventName)) {
      throw new Error(`Event ${eventName} is not registered.`);
    }
    return this.eventsMap.get(eventName) as event_types.EmitterEvent;
  }

  get listEvents(): event_types.EmitterEngineEventNames[] {
    return this.emitter.eventNames() as event_types.EmitterEngineEventNames[];
  }

  bindEvent(event: event_types.EmitterEvent): void {
    const eventNames = this.emitter.eventNames();
    if (!eventNames.includes(event.name)) {
      logger.info('Registering event:', event.name);
      this.eventsMap.set(event.name, event);
      this.emitter.on(event.name, event.handler.bind(event));
    } else {
      logger.warn(`Event ${event.name} is already registered. Skipping.`);
    }
  }

  emitEvent(eventName: event_types.EmitterEngineEventNames, data: any): void {
    if (!this.emitter.eventNames().includes(eventName)) {
      logger.error(`Event ${eventName} is not registered.`);
    } else {
      this.emitter.emit(eventName, { ...data, type: eventName });
    }
  }

  shutDown(): void {
    this.emitter.removeAllListeners();
    logger.info('Emitter engine shut down and all listeners removed.');
  }
}
