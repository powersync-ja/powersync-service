import { event_types } from '@powersync/service-types';

export interface ReportStorage extends AsyncDisposable {
  reportClientConnection(data: event_types.ClientConnectionBucketData): Promise<void>;
  reportClientDisconnection(data: event_types.ClientDisconnectionEventData): Promise<void>;
  getConnectedClients(data: event_types.ClientConnectionsRequest): Promise<event_types.ClientConnectionReport>;
  getClientConnectionReports(
    data: event_types.ClientConnectionReportRequest
  ): Promise<event_types.ClientConnectionReport>;
  deleteOldConnectionData(data: event_types.DeleteOldConnectionData): Promise<void>;
}
