declare module '@vlasky/zongji' {
  export type ZongjiOptions = {
    host: string;
    user: string;
    password: string;
  };

  export type StartOptions = {
    includeEvents?: string[];
    excludeEvents?: string[];
    /**
     * BinLog position filename to start reading events from
     */
    filename?: string;
    /**
     * BinLog position offset to start reading events from in file specified
     */
    position?: number;
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
