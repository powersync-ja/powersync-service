import { replication } from '@powersync/service-core';
import mysql_promise from 'mysql2/promise';
import * as types from '../types/types.js';
import * as mysql_utils from '../utils/mysql_utils.js';

export class MSSQLReplicator implements replication.Replicator {
  id: string;

  protected pool: mysql_promise.Pool;

  constructor(protected config: types.ResolvedConnectionConfig) {
    this.id = 'mysql';

    this.pool = mysql_utils.createPool(config);
  }

  async start(): Promise<void> {
    console.log('starting');
  }
  async stop(): Promise<void> {
    // throw new Error('Method not implemented.');
  }
}
