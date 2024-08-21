import { replication, system } from '@powersync/service-core';
import mysql_promise from 'mysql2/promise';
import * as types from '../types/types.js';
import * as mysql_utils from '../utils/mysql_utils.js';
import { MysqlBinLogStreamManager } from './MysqlBinLogStreamManager.js';

export class MSSQLReplicator implements replication.Replicator {
  id: string;

  protected pool: mysql_promise.Pool;
  protected manager: MysqlBinLogStreamManager | null;

  constructor(protected config: types.ResolvedConnectionConfig, protected serviceContext: system.ServiceContext) {
    this.id = 'mysql';
    this.pool = mysql_utils.createPool(config);
    this.manager = null;
  }

  async start(): Promise<void> {
    console.log('starting');
    this.manager = new MysqlBinLogStreamManager(this.serviceContext, this.pool);

    this.manager.start();
    // const zongji = new ZongJi({
    //   host: this.config.hostname,
    //   user: this.config.username,
    //   password: this.config.password
    //   // debug: true
    // });

    // zongji.on('binlog', function (evt: any) {
    //   // evt.dump();
    //   console.log(evt);
    // });

    // zongji.start({
    //   includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows']
    // });

    // process.on('SIGINT', function () {
    //   console.log('Got SIGINT.');
    //   zongji.stop();
    // });
  }
  async stop(): Promise<void> {
    await this.manager?.stop();
    // throw new Error('Method not implemented.');
  }
}
