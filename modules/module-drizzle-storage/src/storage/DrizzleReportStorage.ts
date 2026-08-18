import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';

export class DrizzleReportStorage implements storage.ReportStorage {
  async [Symbol.asyncDispose](): Promise<void> {
    // Report storage is intentionally a no-op in the initial Drizzle storage slice.
  }

  async reportClientConnection(_data: event_types.ClientConnectionBucketData): Promise<void> {}

  async reportClientDisconnection(_data: event_types.ClientDisconnectionEventData): Promise<void> {}

  async getConnectedClients(): Promise<event_types.ClientConnectionReportResponse> {
    return { users: [], sdks: [] } as unknown as event_types.ClientConnectionReportResponse;
  }

  async getClientConnectionReports(
    _data: event_types.ClientConnectionReportRequest
  ): Promise<event_types.ClientConnectionReportResponse> {
    return { users: [], sdks: [] } as unknown as event_types.ClientConnectionReportResponse;
  }

  async getGeneralClientConnectionAnalytics(
    _data: event_types.ClientConnectionAnalyticsRequest
  ): Promise<event_types.PaginatedResponse<event_types.ClientConnection>> {
    return {
      items: [],
      count: 0,
      more: false
    } as unknown as event_types.PaginatedResponse<event_types.ClientConnection>;
  }

  async deleteOldConnectionData(_data: event_types.DeleteOldConnectionData): Promise<void> {}
}
