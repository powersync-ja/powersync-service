import {
  InstanceRequest,
  ListCurrentConnectionsResponse,
  SdkConnectDocument,
  SdkUserData
} from '@powersync/service-types/dist/events.js';

// Interface for the ReportStorageFactory
export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: SdkConnectDocument): Promise<void>;
  reportSdkDisconnect(data: SdkUserData): Promise<void>;
  listCurrentConnections(data: InstanceRequest): Promise<ListCurrentConnectionsResponse>;
  scrapeSdkData(data: InstanceRequest): Promise<ListCurrentConnectionsResponse>;
}
