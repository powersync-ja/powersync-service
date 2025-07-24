import { storage } from '@powersync/service-core';
import * as pg_wire from '@powersync/service-jpgwire';

import * as lib_postgres from '@powersync/lib-service-postgres';
import { NormalizedPostgresStorageConfig } from '../types/types.js';

import { STORAGE_SCHEMA_NAME } from '../utils/db.js';
import { getStorageApplicationName } from '../utils/application-name.js';
import {
  DeleteOldSdkData,
  ListCurrentConnections,
  ListCurrentConnectionsRequest,
  ScrapeSdkDataRequest,
  SdkConnectBucketData,
  SdkDisconnectEventData
} from '@powersync/service-types/src/events.js';

export type PostgresReportStorageOptions = {
  config: NormalizedPostgresStorageConfig;
};

export class PostgresReportStorageFactory implements storage.ReportStorageFactory {
  readonly db: lib_postgres.DatabaseClient;
  private activeStorage: storage.SyncRulesBucketStorage | undefined;

  constructor(protected options: PostgresReportStorageOptions) {
    this.db = new lib_postgres.DatabaseClient({
      config: options.config,
      schema: STORAGE_SCHEMA_NAME,
      applicationName: getStorageApplicationName()
    });

    this.db.registerListener({
      connectionCreated: async (connection) => this.prepareStatements(connection)
    });
  }
  reportSdkConnect(data: SdkConnectBucketData): Promise<void> {
    throw new Error('Method not implemented.');
  }
  reportSdkDisconnect(data: SdkDisconnectEventData): Promise<void> {
    throw new Error('Method not implemented.');
  }
  listCurrentConnections(data: ListCurrentConnectionsRequest): Promise<ListCurrentConnections> {
    throw new Error('Method not implemented.');
  }
  scrapeSdkData(data: ScrapeSdkDataRequest): Promise<ListCurrentConnections> {
    throw new Error('Method not implemented.');
  }
  deleteOldSdkData(data: DeleteOldSdkData): Promise<void> {
    throw new Error('Method not implemented.');
  }

  async [Symbol.asyncDispose]() {
    await this.db[Symbol.asyncDispose]();
  }

  async prepareStatements(connection: pg_wire.PgConnection) {
    // It should be possible to prepare statements for some common operations here.
    // This has not been implemented yet.
  }
}
