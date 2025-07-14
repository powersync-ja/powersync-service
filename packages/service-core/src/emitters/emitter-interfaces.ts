// export type SdkEvent = {
//   client_id?: string;
//   user_id: string;
//   user_agent: string;
//   jwt_token?: JwtPayload;
// };
//
// export type EventConnectData = {
//   type: EmitterEngineEventNames.SDK_CONNECT_EVENT;
//   connect_at: number;
// } & SdkEvent;
// export type EventDisconnectData = {
//   type: EmitterEngineEventNames.SDK_DISCONNECT_EVENT;
//   disconnect_at: number;
// } & SdkEvent;

import { event_types } from '@powersync/service-types';

export interface BaseEmitterEngine {
  events: event_types.EmitterEngineEventNames[];
  bindEvent(events: event_types.EmitterEvent): void;
  eventNames(): event_types.EmitterEngineEventNames[];
  emitEvent(eventName: event_types.EmitterEngineEventNames, data: any): void;
  getStoredEvent(eventName: event_types.EmitterEngineEventNames): event_types.EmitterEvent;
  shutDown(): void;
}
