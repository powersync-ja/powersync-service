import { event_types } from '@powersync/service-types';

export interface BaseEmitterEngine {
  listEvents: event_types.EmitterEngineEvents[];
  countListeners(eventName: event_types.EmitterEngineEvents): number;
  emit<K extends event_types.EmitterEngineEvents>(
    event: event_types.EmitterEngineEvents,
    data: event_types.SubscribeEvents[K]
  ): void;
  subscribe<K extends event_types.EmitterEngineEvents>(event: event_types.EmitterEvent<K>): void;
  shutDown(): void;
}
