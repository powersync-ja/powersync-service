import {
  DeleteOldSdkData,
  InstanceRequest,
  ListCurrentConnectionsResponse,
  SdkConnectBucketData,
  SdkDisconnectEventData
} from '@powersync/service-types/dist/events.js';

// Interface for the ReportStorageFactory
export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: SdkConnectBucketData): Promise<void>;
  reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void>;
  listCurrentConnections(data: InstanceRequest): Promise<ListCurrentConnectionsResponse>;
  scrapeSdkData(data: InstanceRequest): Promise<ListCurrentConnectionsResponse>;
  deleteOldSdkData(data: DeleteOldSdkData): Promise<void>;
}
