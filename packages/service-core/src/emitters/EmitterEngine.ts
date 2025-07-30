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
      logger.error(error.message, { stack: error.stack });
    });
  }

  subscribe<K extends event_types.EmitterEngineEvents>(event: event_types.EmitterEvent<K>): void {
    if (!this.events.has(event.event)) {
      this.events.add(event.event);
    }
    this.emitter.on(event.event, event.handler.bind(event));
  }

  get listEvents(): event_types.EmitterEngineEvents[] {
    return Array.from(this.events.values());
  }

  countListeners(eventName: event_types.EmitterEngineEvents): number {
    return this.emitter.listenerCount(eventName);
  }

  emit<K extends keyof event_types.SubscribeEvents>(event: K, data: event_types.SubscribeEvents[K]): void {
    if (!this.events.has(event as event_types.EmitterEngineEvents)) {
      this.emitter.emit('error', new Error(`There are no subscribed events for "${event}".`));
    } else {
      this.emitter.emit(event, data);
    }
  }

  shutDown(): void {
    logger.info(`Shutting down EmitterEngine and removing all listeners for ${this.listEvents.join(', ')}.`);
    this.emitter.removeAllListeners();
  }
}
