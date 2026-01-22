import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { SourceTable, SourceTableOptions } from '@powersync/service-core';

export class PostgresSourceTable extends SourceTable {
  public readonly groupId: number;

  constructor(options: SourceTableOptions, postgresOptions: { groupId: number }) {
    super(options);
    this.groupId = postgresOptions.groupId;

    if (typeof options.id != 'string') {
      throw new ReplicationAssertionError('PostgresSourceTable id must be a string');
    }
  }

  get id() {
    return this.options.id as string;
  }

  clone(): PostgresSourceTable {
    const copy = new PostgresSourceTable(
      {
        id: this.id,
        connectionTag: this.connectionTag,
        objectId: this.objectId,
        schema: this.schema,
        name: this.name,
        replicaIdColumns: this.replicaIdColumns,
        snapshotComplete: this.snapshotComplete,
        pattern: this.pattern,
        bucketDataSourceIds: this.bucketDataSourceIds,
        parameterLookupSourceIds: this.parameterLookupSourceIds
      },
      { groupId: this.groupId }
    );
    copy.syncData = this.syncData;
    copy.syncParameters = this.syncParameters;
    copy.snapshotStatus = this.snapshotStatus;
    return copy;
  }
}
