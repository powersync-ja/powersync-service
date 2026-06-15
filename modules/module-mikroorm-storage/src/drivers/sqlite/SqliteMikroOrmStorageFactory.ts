import { MikroORM } from '@mikro-orm/core';
import { MikroOrmBucketStorageFactory } from '../../storage/MikroOrmBucketStorageFactory.js';
import { NormalizedMikroOrmSqliteStorageConfig } from '../../types/types.js';
import { createSqliteMikroOrm } from './sqlite-config.js';
import { sqliteMikroOrmStorageDialect } from './sqlite-dialect.js';

export async function createSqliteMikroOrmStorageFactory(options: {
  config: NormalizedMikroOrmSqliteStorageConfig;
  slotNamePrefix: string;
  orm?: MikroORM;
}): Promise<MikroOrmBucketStorageFactory> {
  const orm = options.orm ?? (await createSqliteMikroOrm(options.config));
  return new MikroOrmBucketStorageFactory({
    orm,
    dialect: sqliteMikroOrmStorageDialect,
    slotNamePrefix: options.slotNamePrefix
  });
}
