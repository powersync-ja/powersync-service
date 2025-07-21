export enum EmitterEngineEvents {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event'
}

export type SubscribeEvents = {
  [EmitterEngineEvents.SDK_CONNECT_EVENT]: SdkConnectEventData;
  [EmitterEngineEvents.SDK_DISCONNECT_EVENT]: SdkDisconnectEventData;
};

export type SdkUserData = {
  client_id?: string;
  user_id: string;
  user_agent?: string;
  jwt_exp?: Date;
};

export type SdkConnectEventData = {
  connect_at: Date;
} & SdkUserData;

export type SdkDisconnectEventData = {
  disconnect_at: Date;
} & SdkUserData;

export type SdkConnectDocument = {
  sdk: string;
  version: string;
  user_agent: string;
  client_id: string;
  user_id: string;
  jwt_exp?: Date;
  connect_at?: Date;
  disconnect_at?: Date;
};

export type PaginatedInstanceRequest = {
  app_id: string;
  org_id: string;
  cursor?: string;
  limit?: number;
};
export type EventHandlerFunc<K extends EmitterEngineEvents> = (data: SubscribeEvents[K]) => Promise<void> | void;
export interface EmitterEvent<K extends EmitterEngineEvents> {
  event: K;
  handler: EventHandlerFunc<K>;
}
