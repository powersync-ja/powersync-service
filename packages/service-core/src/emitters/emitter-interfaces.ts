import { JwtPayload } from '../auth/JwtPayload.js';
import * as storage from '../storage/storage-index.js';
import { EventError } from './event-error.js';

export enum EventNames {
  // SdK events
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event'
}

export type SdkEvent = {
  client_id?: string;
  user_id: string;
  user_agent: string;
  jwt_token?: JwtPayload;
};

export type EventConnectData = {
  type: EventNames.SDK_CONNECT_EVENT;
  connect_at: number;
} & SdkEvent;

export type EventDisconnectData = {
  type: EventNames.SDK_DISCONNECT_EVENT;
  disconnect_at: number;
} & SdkEvent;

export type EmitterEventData = EventConnectData | EventDisconnectData;
export type EventHandler = (data: EmitterEventData) => Promise<void>;
export type StorageHandler = (storageEngine: storage.StorageEngine) => EventHandler;
export interface EmitterEvent {
  name: EventNames;
  handler: StorageHandler;
  errorhandler?: (error: Error | EventError) => void;
}

export interface BaseEmitterEngine {
  eventNames(): EventNames[];
  emitEvent(eventName: EventNames, data: EmitterEventData): void;
  removeListeners(eventName?: EventNames): void;
  event(eventName: EventNames): EmitterEvent;
  shutDown(): Promise<void>;
}

export function isEventError(error: Error | EventError): error is EventError {
  return (error as EventError).eventName !== undefined;
}
