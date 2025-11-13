import EventEmitter from 'node:events';
import { logger } from '@powersync/lib-services-framework';
import { event_types } from '@powersync/service-types';

export class EventsEngine {
  private emitter: EventEmitter;
  private events: Set<event_types.EventsEngineEventType> = new Set();
  constructor() {
    this.emitter = new EventEmitter({ captureRejections: true });
    this.emitter.on('error', (error: Error) => {
      logger.error(error.message);
    });
  }

  /**
   * All new events added need to be subscribed to be used.
   * @example engine.subscribe(new MyNewEvent(storageEngine));
   */
  subscribe<K extends event_types.EventsEngineEventType>(event: event_types.EmitterEvent<K>): void {
    if (!this.events.has(event.event)) {
      this.events.add(event.event);
    }
    this.emitter.on(event.event, event.handler.bind(event));
  }

  get listEvents(): event_types.EventsEngineEventType[] {
    return Array.from(this.events.values());
  }

  emit<K extends keyof event_types.SubscribeEvents>(event: K, data: event_types.SubscribeEvents[K]): void {
    this.emitter.emit(event, data);
  }

  shutDown(): void {
    logger.info(`Shutting down EmitterEngine and removing all listeners for ${this.listEvents.join(', ')}.`);
    this.emitter.removeAllListeners();
  }
}
