export enum EmitterEngineEventNames {
  SDK_CONNECT_EVENT = 'sdk-connect-event',
  SDK_DISCONNECT_EVENT = 'sdk-disconnect-event'
}

export type EventHandlerFunc = (data: any) => Promise<void> | ((controller: any) => (data: any) => void);
export interface EmitterEvent {
  name: EmitterEngineEventNames;
  handler: EventHandlerFunc;
}
