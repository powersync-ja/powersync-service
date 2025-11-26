import { ColumnDescriptor } from '@powersync/service-core';
import { ISqlType } from 'mssql';

export interface MSSQLColumnDescriptor extends ColumnDescriptor {
  /** The underlying system type id. For base types system type id == user type id */
  typeId: number;
  /** The unique user type id that uniquely identifies the type  */
  userTypeId: number;
  // /** The name of the user/alias type. For example SYSNAME, GEOMETRY */
  // userTypeName: string;
}

/** The shared system type id for all CLR_UDT types in SQL Server */
export const CLR_UDT_TYPE_ID = 240;

/**
 *  Enum mapping the base MSSQL data types to their system type IDs.
 */
export enum MSSQLBaseType {
  IMAGE = 34,
  TEXT = 35,
  UNIQUEIDENTIFIER = 36,
  DATE = 40,
  TIME = 41,
  DATETIME2 = 42,
  DATETIMEOFFSET = 43,
  TINYINT = 48,
  SMALLINT = 52,
  INT = 56,
  SMALLDATETIME = 58,
  REAL = 59,
  MONEY = 60,
  DATETIME = 61,
  FLOAT = 62,
  SQL_VARIANT = 98,
  NTEXT = 99,
  BIT = 104,
  DECIMAL = 106,
  NUMERIC = 108,
  SMALLMONEY = 122,
  BIGINT = 127,
  VARBINARY = 165,
  VARCHAR = 167,
  BINARY = 173,
  CHAR = 175,
  TIMESTAMP = 189,
  NVARCHAR = 231,
  NCHAR = 239,
  XML = 241,
  JSON = 244
}

/**
 *  Enum mapping some of the extended user-defined MSSQL data types to their user type IDs.
 */
export enum MSSQLExtendedUserType {
  // VARBINARY system type [155]
  VECTOR = 255,
  // NVARCHAR system type [231]
  SYSNAME = 256,
  // CLR_UDT system type [240]
  HIERARCHYID = 128,
  // CLR_UDT system type [240]
  GEOMETRY = 129,
  // CLR_UDT system type [240]
  GEOGRAPHY = 130
}

export enum MSSQLUserDefinedType {
  VECTOR = 'vector',
  SYSNAME = 'sysname',
  HIERARCHYID = 'hierarchyid'
}

export interface MSSQLParameter {
  name: string;
  value: any;
  type?: (() => ISqlType) | ISqlType;
}
