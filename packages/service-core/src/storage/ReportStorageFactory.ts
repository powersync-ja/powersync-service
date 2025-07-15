import {
  CurrentConnectionsData,
  SdkConnectEventData,
  SdkDisconnectEventData
} from '@powersync/service-types/dist/events.js';

export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: SdkConnectEventData): Promise<void>;
  reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void>;
  listCurrentConnections(data: CurrentConnectionsData): Promise<void>;
}
