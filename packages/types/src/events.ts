export enum EmitterEngineEvents {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event',
  SDK_DELETE_OLD = 'sdk-delete-old'
}

export type SubscribeEvents = {
  [EmitterEngineEvents.SDK_CONNECT_EVENT]: SdkConnectEventData;
  [EmitterEngineEvents.SDK_DISCONNECT_EVENT]: SdkDisconnectEventData;
  [EmitterEngineEvents.SDK_DELETE_OLD]: DeleteOldSdkData;
};

export type SdkUserData = {
  client_id?: string;
  user_id: string;
  user_agent?: string;
  jwt_exp?: Date;
};

export type DeleteOldSdkData = {
  interval: number;
  timeframe: 'month' | 'week' | 'day';
};

export type SdkConnectEventData = {
  connect_at: Date;
} & SdkUserData;

export type SdkConnectBucketData = {
  connect_at: Date;
  sdk: string;
} & SdkUserData;

export type SdkDisconnectEventData = {
  disconnect_at: Date;
} & SdkUserData;

export type SdkConnectDocument = {
  sdk: string;
  user_agent: string;
  client_id: string;
  user_id: string;
  jwt_exp?: Date;
  connect_at: Date;
};

export type InstanceRequest = {
  app_id: string;
  org_id: string;
};

export type ListCurrentConnections = {
  user_count: number;
  client_count: number;
  user_per_sdk: {
    [sdk_version: string]: number;
  };
};

export type ListCurrentConnectionsResponse = ListCurrentConnections & InstanceRequest;

export type EventHandlerFunc<K extends EmitterEngineEvents> = (data: SubscribeEvents[K]) => Promise<void> | void;
export interface EmitterEvent<K extends EmitterEngineEvents> {
  event: K;
  handler: EventHandlerFunc<K>;
}
