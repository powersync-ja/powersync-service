import { replication } from '@powersync/service-core';
//@ts-ignore
import ZongJi from '@vlasky/zongji';
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
    const zongji = new ZongJi({
      host: this.config.hostname,
      user: this.config.username,
      password: this.config.password
      // debug: true
    });

    zongji.on('binlog', function (evt: any) {
      // evt.dump();
      console.log(evt);
    });

    zongji.start({
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
    });

    process.on('SIGINT', function () {
      console.log('Got SIGINT.');
      zongji.stop();
    });
  }
  async stop(): Promise<void> {
    // throw new Error('Method not implemented.');
  }
}
