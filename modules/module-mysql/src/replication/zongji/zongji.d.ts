declare module '@powersync/mysql-zongji' {
  export type ZongjiOptions = {
    host: string;
    user: string;
    password: string;
    dateStrings?: boolean;
    timeZone?: string;
  };

  interface DatabaseFilter {
    [databaseName: string]: string[] | true;
  }

  export type StartOptions = {
    includeEvents?: string[];
    excludeEvents?: string[];
    /**
     *  Describe which databases and tables to include (Only for row events). Use database names as the key and pass an array of table names or true (for the entire database).
     *  Example: { 'my_database': ['allow_table', 'another_table'], 'another_db': true }
     */
    includeSchema?: DatabaseFilter;
    /**
     *  Object describing which databases and tables to exclude (Same format as includeSchema)
     *  Example: { 'other_db': ['disallowed_table'], 'ex_db': true }
     */
    excludeSchema?: DatabaseFilter;
    /**
     * BinLog position filename to start reading events from
     */
    filename?: string;
    /**
     * BinLog position offset to start reading events from in file specified
     */
    position?: number;

    /**
     *  Unique server ID for this replication client.
     */
    serverId?: number;
  };

  export type ColumnSchema = {
    COLUMN_NAME: string;
    COLLATION_NAME: string;
    CHARACTER_SET_NAME: string;
    COLUMN_COMMENT: string;
    COLUMN_TYPE: string;
  };

  export type ColumnDefinition = {
    name: string;
    charset: string;
    type: number;
    metadata: Record<string, any>;
  };

  export type TableMapEntry = {
    columnSchemas: Array<ColumnSchema>;
    parentSchema: string;
    tableName: string;
    columns: Array<ColumnDefinition>;
  };

  export type BaseBinLogEvent = {
    timestamp: number;
    getEventName(): string;

    /**
     * Next position in BinLog file to read from after
     * this event.
     */
    nextPosition: number;
    /**
     * Size of this event
     */
    size: number;
    flags: number;
    useChecksum: boolean;
  };

  export type BinLogRotationEvent = BaseBinLogEvent & {
    binlogName: string;
    position: number;
  };

  export type BinLogGTIDLogEvent = BaseBinLogEvent & {
    serverId: Buffer;
    transactionRange: number;
  };

  export type BinLogXidEvent = BaseBinLogEvent & {
    xid: number;
  };

  export type BinLogMutationEvent = BaseBinLogEvent & {
    tableId: number;
    numberOfColumns: number;
    tableMap: Record<string, TableMapEntry>;
    rows: Array<Record<string, any>>;
  };

  export type BinLogUpdateEvent = Omit<BinLogMutationEvent, 'rows'> & {
    rows: Array<{
      before: Record<string, any>;
      after: Record<string, any>;
    }>;
  };

  export type BinLogEvent = BinLogRotationEvent | BinLogGTIDLogEvent | BinLogXidEvent | BinLogMutationEvent;

  export default class ZongJi {
    constructor(options: ZongjiOptions);

    start(options: StartOptions): void;
    stop(): void;

    on(type: 'binlog' | string, callback: (event: BinLogEvent) => void);
  }
}
