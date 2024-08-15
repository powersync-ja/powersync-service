import { replication } from '@powersync/service-core';

export class MSSQLReplicator implements replication.Replicator {
  id: string = '';
  start(): void {
    throw new Error('Method not implemented.');
  }
  stop(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
