import { customType } from 'drizzle-orm/sqlite-core';

/**
 * SQLite INTEGER column whose application type is always bigint.
 *
 * The better-sqlite3 connections are configured with safe integers so the
 * driver value reaches this mapper without first losing precision.
 */
export const sqliteBigInt = customType<{ data: bigint; driverData: bigint | number }>({
  dataType() {
    return 'bigint';
  },
  toDriver(value) {
    return value;
  },
  fromDriver(value) {
    return BigInt(value);
  }
});

export const sqliteInteger = customType<{ data: number; driverData: bigint | number }>({
  dataType() {
    return 'integer';
  },
  toDriver(value) {
    return value;
  },
  fromDriver(value) {
    return Number(value);
  }
});

export const sqliteBoolean = customType<{ data: boolean; driverData: bigint | number }>({
  dataType() {
    return 'integer';
  },
  toDriver(value) {
    return value ? 1 : 0;
  },
  fromDriver(value) {
    return value !== 0n && value !== 0;
  }
});

export const sqliteTimestampMs = customType<{ data: Date; driverData: bigint | number }>({
  dataType() {
    return 'integer';
  },
  toDriver(value) {
    return value.getTime();
  },
  fromDriver(value) {
    return new Date(Number(value));
  }
});
