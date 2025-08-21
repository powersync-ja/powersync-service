export enum EventsEngineEventType {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event',
  SDK_DELETE_OLD = 'sdk-delete-old'
}

export type SubscribeEvents = {
  [EventsEngineEventType.SDK_CONNECT_EVENT]: ClientConnectionEventData;
  [EventsEngineEventType.SDK_DISCONNECT_EVENT]: ClientDisconnectionEventData;
  [EventsEngineEventType.SDK_DELETE_OLD]: DeleteOldConnectionData;
};

export type EventHandlerFunc<K extends EventsEngineEventType> = (data: SubscribeEvents[K]) => Promise<void> | void;
export interface EmitterEvent<K extends EventsEngineEventType> {
  event: K;
  handler: EventHandlerFunc<K>;
}

export type ConnectedUserData = {
  client_id?: string;
  user_id: string;
  user_agent?: string;
  jwt_exp?: Date;
};

export type DeleteOldConnectionData = {
  date: Date;
};

export type ClientConnectionEventData = {
  connected_at: Date;
} & ConnectedUserData;

export type ClientConnectionBucketData = {
  connected_at: Date;
  sdk: string;
} & ConnectedUserData;

export type ClientDisconnectionEventData = {
  disconnected_at: Date;
  connected_at: Date;
} & ConnectedUserData;

export type ClientConnection = {
  id?: string;
  sdk: string;
  user_agent: string;
  client_id: string;
  user_id: string;
  jwt_exp?: Date;
  connected_at: Date;
  disconnected_at?: Date;
};

export type ClientConnectionReport = {
  users: number;
  sdks: {
    sdk: string;
    users: number;
    clients: number;
  }[];
};

export type ClientConnectionReportRequest = {
  start: Date;
  end: Date;
};

export type ClientConnectionsRequest = {
  range?: {
    start: string;
    end?: string;
  };
};
