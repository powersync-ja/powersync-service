export enum EmitterEngineEvents {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event'
}
type JwtExp = {
  exp?: number;
};

export type SubscribeEvents = {
  [EmitterEngineEvents.SDK_CONNECT_EVENT]: SdkConnectEventData;
  [EmitterEngineEvents.SDK_DISCONNECT_EVENT]: SdkDisconnectEventData;
};

export type SdkUserData = {
  client_id?: string;
  user_id: string;
  user_agent?: string;
  jwt_exp: JwtExp;
};

export type SdkConnectEventData = {
  connect_at: number;
} & SdkUserData;

export type SdkDisconnectEventData = {
  disconnect_at: number;
} & SdkUserData;

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
