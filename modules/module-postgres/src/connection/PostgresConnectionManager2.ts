import * as pgwire from '@powersync/service-jpgwire';

import { replication } from '@powersync/service-core';
import { PostgresConnectionConfig } from '../types/types.js';

export type PostgresConnection = pgwire.PgClient;

export class PostgresConnectionManager implements replication.ConnectionManager<PostgresConnection> {
  constructor(protected config: PostgresConnectionConfig) {}

  createReplicationConnection(): Promise<PostgresConnection> {
    throw new Error('Method not implemented.');
  }

  createConnection(): Promise<PostgresConnection> {
    throw new Error('Method not implemented.');
  }

  mapError(error: Error): replication.ConnectionError {
    throw new Error('Method not implemented.');
  }
}
