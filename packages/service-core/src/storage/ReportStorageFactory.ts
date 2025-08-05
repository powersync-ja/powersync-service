import { event_types } from '@powersync/service-types';

export interface ReportStorageFactory extends AsyncDisposable {
  reportSdkConnect(data: event_types.SdkConnectBucketData): Promise<void>;
  reportSdkDisconnect(data: event_types.SdkDisconnectEventData): Promise<void>;
  listCurrentConnections(data: event_types.ListCurrentConnectionsRequest): Promise<event_types.ListCurrentConnections>;
  scrapeSdkData(data: event_types.ScrapeSdkDataRequest): Promise<event_types.ListCurrentConnections>;
  deleteOldSdkData(data: event_types.DeleteOldSdkData): Promise<void>;
}
