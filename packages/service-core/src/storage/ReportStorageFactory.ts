import { PaginatedInstanceRequest, SdkConnectDocument } from '@powersync/service-types/dist/events.js';

export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: SdkConnectDocument): Promise<void>;
  reportSdkDisconnect(data: SdkConnectDocument): Promise<void>;
  listCurrentConnections(data: PaginatedInstanceRequest): Promise<void>;
  scrapeSdkData(data: PaginatedInstanceRequest): Promise<void>;
}
