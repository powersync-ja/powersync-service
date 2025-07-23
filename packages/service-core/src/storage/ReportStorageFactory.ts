import { event_types } from '@powersync/service-types';

export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: event_types.SdkConnectBucketData): Promise<void>;
  reportSdkDisconnect(data: event_types.SdkDisconnectEventData): Promise<void>;
  listCurrentConnections(
    data: event_types.ListCurrentConnectionsRequest
  ): Promise<event_types.ListCurrentConnectionsResponse>;
  scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnectionsResponse>;
  deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void>;
}
