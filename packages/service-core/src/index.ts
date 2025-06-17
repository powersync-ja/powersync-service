// Provides global and namespaced exports

export * from './api/api-index.js';
export * as api from './api/api-index.js';

export * from './auth/auth-index.js';
export * as auth from './auth/auth-index.js';

export * from './entry/entry-index.js';
export * as entry from './entry/entry-index.js';

// Re-export framework for easy use of Container API
export * as framework from '@powersync/lib-services-framework';

export * from './metrics/metrics-index.js';
export * as metrics from './metrics/metrics-index.js';

export * from './migrations/migrations-index.js';
export * as migrations from './migrations/migrations-index.js';

export * from './modules/modules-index.js';
export * as modules from './modules/modules-index.js';

export * from './replication/replication-index.js';
export * as replication from './replication/replication-index.js';

export * from './routes/routes-index.js';
export * as routes from './routes/routes-index.js';

export * from './storage/storage-index.js';
export * as storage from './storage/storage-index.js';

export * from './sync/sync-index.js';
export * as sync from './sync/sync-index.js';

export * from './system/system-index.js';
export * as system from './system/system-index.js';

export * from './util/util-index.js';
export * as utils from './util/util-index.js';

export * from './streams/streams-index.js';
export * as streams from './streams/streams-index.js';

export * as bson from 'bson';
