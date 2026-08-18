import {
  BucketDataSchema,
  BucketParametersSchema,
  CurrentDataSchema,
  SyncRulesSchema
} from './entities-index.js';

const MYSQL_INDEXED_BINARY_COLUMN_TYPE = 'varbinary(1024)';
const MYSQL_LONG_TEXT_COLUMN_TYPE = 'longtext';
const MYSQL_LONG_BINARY_COLUMN_TYPE = 'longblob';

export function configureDefaultEntityColumnTypes(): void {
  setColumnType(BucketDataSchema, 'sourceKey', undefined);
  setColumnType(BucketDataSchema, 'data', undefined);
  setColumnType(BucketParametersSchema, 'sourceKey', undefined);
  setColumnType(BucketParametersSchema, 'lookup', undefined);
  setColumnType(CurrentDataSchema, 'sourceKey', undefined);
  setColumnType(CurrentDataSchema, 'data', undefined);
  setColumnType(SyncRulesSchema, 'content', undefined);
}

export function configureMySqlEntityColumnTypes(): void {
  setColumnType(BucketDataSchema, 'sourceKey', MYSQL_INDEXED_BINARY_COLUMN_TYPE);
  setColumnType(BucketDataSchema, 'data', MYSQL_LONG_TEXT_COLUMN_TYPE);
  setColumnType(BucketParametersSchema, 'sourceKey', MYSQL_INDEXED_BINARY_COLUMN_TYPE);
  setColumnType(BucketParametersSchema, 'lookup', MYSQL_INDEXED_BINARY_COLUMN_TYPE);
  setColumnType(CurrentDataSchema, 'sourceKey', MYSQL_INDEXED_BINARY_COLUMN_TYPE);
  setColumnType(CurrentDataSchema, 'data', MYSQL_LONG_BINARY_COLUMN_TYPE);
  setColumnType(SyncRulesSchema, 'content', MYSQL_LONG_TEXT_COLUMN_TYPE);
}

function setColumnType(schema: { properties: Record<string, unknown> }, propertyName: string, columnType: string | undefined) {
  const property = schema.properties[propertyName] as { columnType?: string } | undefined;
  if (property == null) {
    throw new Error(`Cannot configure missing MikroORM entity property ${propertyName}`);
  }

  if (columnType == null) {
    delete property.columnType;
  } else {
    property.columnType = columnType;
  }
}
