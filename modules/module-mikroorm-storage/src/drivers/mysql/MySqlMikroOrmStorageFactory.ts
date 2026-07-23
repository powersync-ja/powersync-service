import { MikroORM } from '@mikro-orm/core';
import { MikroOrmBucketStorageFactory } from '../../storage/MikroOrmBucketStorageFactory.js';
import { NormalizedMikroOrmMySqlStorageConfig } from '../../types/types.js';
import { createMySqlMikroOrm } from './mysql-config.js';
import { mysqlMikroOrmStorageDialect } from './mysql-dialect.js';

export async function createMySqlMikroOrmStorageFactory(options: {
  config: NormalizedMikroOrmMySqlStorageConfig;
  slotNamePrefix: string;
  orm?: MikroORM;
}): Promise<MikroOrmBucketStorageFactory> {
  const orm = options.orm ?? (await createMySqlMikroOrm(options.config));
  return new MikroOrmBucketStorageFactory({
    orm,
    dialect: mysqlMikroOrmStorageDialect,
    slotNamePrefix: options.slotNamePrefix
  });
}
