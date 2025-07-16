import EventEmitter from 'node:events';
import { logger } from '@powersync/lib-services-framework';
import { event_types } from '@powersync/service-types';
import { BaseEmitterEngine } from './emitter-interfaces.js';

export class EmitterEngine implements BaseEmitterEngine {
  private emitter: EventEmitter;
  events: Set<event_types.EmitterEngineEvents> = new Set();
  constructor() {
    this.emitter = new EventEmitter({ captureRejections: true });
    this.emitter.on('error', (error: Error) => {
      logger.error(error);
    });
  }

  subscribe<K extends event_types.EmitterEngineEvents>(event: event_types.EmitterEvent<K>): void {
    if (!this.events.has(event.name)) {
      this.events.add(event.name);
    }
    this.emitter.on(event.name, event.handler.bind(event));
  }

  get listEvents(): event_types.EmitterEngineEvents[] {
    return this.emitter.eventNames() as event_types.EmitterEngineEvents[];
  }

  countListeners(eventName: event_types.EmitterEngineEvents): number {
    return this.emitter.listenerCount(eventName);
  }

  emit<K extends keyof event_types.SubscribeEvents>(event: K, data: event_types.SubscribeEvents[K]): void {
    if (!this.events.has(event) || this.countListeners(event) === 0) {
      logger.warn(`${event} has no listener registered.`);
    }
    this.emitter.emit(event, { ...data, type: event });
  }

  shutDown(): void {
    logger.info(`Shutting down EmitterEngine and removing all listeners for ${this.listEvents}.`);
    this.emitter.removeAllListeners();
  }
}
