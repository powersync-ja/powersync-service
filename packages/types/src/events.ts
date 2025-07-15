export enum EmitterEngineEventNames {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event'
}
type JwtPayload = {
  sub: string;
  iss?: string | undefined;
  exp: number;
  iat: number;
};

export type SdkConnectEventData = {
  type: EmitterEngineEventNames.SDK_DISCONNECT_EVENT;
  client_id?: string;
  user_id: string;
  connect_at: number;
  user_agent: string;
  jwt_token?: JwtPayload;
};

export type SdkDisconnectEventData = {
  type: EmitterEngineEventNames.SDK_DISCONNECT_EVENT;
  client_id?: string;
  user_id: string;
  disconnect_at: number;
  user_agent: string;
  jwt_token?: JwtPayload;
};

export type CurrentConnectionsData = {
  app_id: string;
  org_id: string;
  id: string;
  cursor: string;
  limit?: number;
};
export type EventHandlerFunc = (data: any) => Promise<void> | void;
export interface EmitterEvent {
  name: EmitterEngineEventNames;
  setController?: (controller: any) => EmitterEvent;
  handler: EventHandlerFunc;
}
