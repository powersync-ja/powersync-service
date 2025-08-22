export enum EventsEngineEventType {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event',
  SDK_DELETE_OLD = 'sdk-delete-old'
}

/**
 * Events engine event types.
 * Any new events will need to be added here with the data structure they expect.
 */
export type SubscribeEvents = {
  [EventsEngineEventType.SDK_CONNECT_EVENT]: ClientConnectionEventData;
  [EventsEngineEventType.SDK_DISCONNECT_EVENT]: ClientDisconnectionEventData;
  [EventsEngineEventType.SDK_DELETE_OLD]: DeleteOldConnectionData;
};

/**
 * Events handler functions
 */
export type EventHandlerFunc<K extends EventsEngineEventType> = (data: SubscribeEvents[K]) => Promise<void> | void;

/**
 * Emitter event interface.
 * Create a class extending EmitterEvent and implement the handler function.
 */
export interface EmitterEvent<K extends EventsEngineEventType> {
  event: K;
  handler: EventHandlerFunc<K>;
}

export type ConnectedUserData = {
  client_id?: string;
  user_id: string;
  user_agent?: string;
  jwt_exp: Date;
};

export type DeleteOldConnectionData = {
  /**
   * Date before which all connection data should be deleted.
   * This is used to clean up old connection data that is no longer needed.
   */
  date: Date;
};

export type ClientConnectionEventData = {
  connected_at: Date;
} & ConnectedUserData;

export type ClientConnectionBucketData = {
  connected_at: Date;
  /** parsed sdk version from the user agent. */
  sdk: string;
} & ConnectedUserData;

export type ClientDisconnectionEventData = {
  disconnected_at: Date;
  connected_at: Date;
} & ConnectedUserData;

/** client connection schema stored locally */
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

export type ClientConnectionReportResponse = {
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
