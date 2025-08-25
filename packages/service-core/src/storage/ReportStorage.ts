import { event_types } from '@powersync/service-types';

/**
 * Represents a configured report storage.
 *
 * Report storage is used for storing localized data for the instance.
 * Data can then be used for reporting purposes.
 *
 */
export interface ReportStorage extends AsyncDisposable {
  /**
   * Report a client connection.
   */
  reportClientConnection(data: event_types.ClientConnectionBucketData): Promise<void>;
  /**
   * Report a client disconnection.
   */
  reportClientDisconnection(data: event_types.ClientDisconnectionEventData): Promise<void>;
  /**
   * Get currently connected clients.
   * This will return any short or long term connected clients.
   * Clients that have no disconnected_at timestamp and that have a valid jwt_exp timestamp are considered connected.
   * @param data.range.start Needs to be UTC time string
   * @param data.range.end Optional needs to be UTC time string
   */
  getConnectedClients(data: event_types.ClientConnectionsRequest): Promise<event_types.ClientConnectionReportResponse>;
  /**
   * Get a report of client connections over a day, week or month.
   * This is internally used to generate reports over it always returns the previous day, week or month.
   * Usually this is call on the start of the new day, week or month. It will return all unique completed connections
   * as well as uniques currently connected clients.
   */
  getClientConnectionReports(
    data: event_types.ClientConnectionReportRequest
  ): Promise<event_types.ClientConnectionReportResponse>;
  /**
   * Delete old connection data based on a specific date.
   * This is used to clean up old connection data that is no longer needed.
   */
  deleteOldConnectionData(data: event_types.DeleteOldConnectionData): Promise<void>;
}
