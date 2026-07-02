import { storage } from '@powersync/service-core';
import { event_types } from '@powersync/service-types';

export class SlateDBReportStorage implements storage.ReportStorage {
  async reportClientConnection(): Promise<void> {
    throw notImplemented('reportClientConnection');
  }

  async reportClientDisconnection(): Promise<void> {
    throw notImplemented('reportClientDisconnection');
  }

  async getConnectedClients(): Promise<event_types.ClientConnectionReportResponse> {
    throw notImplemented('getConnectedClients');
  }

  async getClientConnectionReports(): Promise<event_types.ClientConnectionReportResponse> {
    throw notImplemented('getClientConnectionReports');
  }

  async getGeneralClientConnectionAnalytics(): Promise<event_types.PaginatedResponse<event_types.ClientConnection>> {
    throw notImplemented('getGeneralClientConnectionAnalytics');
  }

  async deleteOldConnectionData(): Promise<void> {
    throw notImplemented('deleteOldConnectionData');
  }

  async [Symbol.asyncDispose](): Promise<void> {
    // No SlateDB resources are opened by the scaffold.
  }
}

function notImplemented(method: string): Error {
  return new Error(`SlateDB report storage scaffold: ${method} is not implemented yet.`);
}
