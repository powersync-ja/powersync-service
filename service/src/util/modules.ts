import * as core from '@powersync/service-core';

export const DYNAMIC_MODULES: core.ModuleLoaders = {
  connection: {
    convex: () => import('@powersync/service-module-convex').then((module) => new module.ConvexModule()),
    mongodb: () => import('@powersync/service-module-mongodb').then((module) => new module.MongoModule()),
    mysql: () => import('@powersync/service-module-mysql').then((module) => new module.MySQLModule()),
    mssql: () => import('@powersync/service-module-mssql').then((module) => new module.MSSQLModule()),
    postgresql: () => import('@powersync/service-module-postgres').then((module) => new module.PostgresModule())
  },
  storage: {
    mongodb: () =>
      import('@powersync/service-module-mongodb-storage').then((module) => new module.MongoStorageModule()),
    postgresql: () =>
      import('@powersync/service-module-postgres-storage').then((module) => new module.PostgresStorageModule())
  }
};
