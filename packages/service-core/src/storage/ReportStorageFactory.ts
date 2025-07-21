import {
  DeleteOldSdkData,
  InstanceRequest,
  ListCurrentConnectionsResponse,
  SdkConnectEventData,
  SdkDisconnectEventData
} from '@powersync/service-types/dist/events.js';

// Interface for the ReportStorageFactory
export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: SdkConnectEventData): Promise<void>;
  reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void>;
  listCurrentConnections(data: InstanceRequest): Promise<ListCurrentConnectionsResponse>;
  scrapeSdkData(data: InstanceRequest): Promise<ListCurrentConnectionsResponse>;
  deleteOldSdkData(data: DeleteOldSdkData): Promise<void>;
}
