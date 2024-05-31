import * as pgwire from '@powersync/service-jpgwire';
import * as micro from '@journeyapps-platform/micro';

import * as auth from '@/auth/auth-index.js';
import * as storage from '@/storage/storage-index.js';
import * as utils from '@/util/util-index.js';

export abstract class CorePowerSyncSystem extends micro.system.MicroSystem {
  abstract storage: storage.BucketStorageFactory;
  abstract client_keystore: auth.KeyStore;
  abstract dev_client_keystore: auth.KeyStore;
  abstract pgwire_pool?: pgwire.PgClient;

  protected stopHandlers: Set<() => void> = new Set();

  closed: boolean;

  constructor(public config: utils.ResolvedPowerSyncConfig) {
    super();
    this.closed = false;
  }

  abstract addTerminationHandler(): void;

  addStopHandler(handler: () => void): () => void {
    if (this.closed) {
      handler();
      return () => {};
    }
    this.stopHandlers.add(handler);
    return () => {
      this.stopHandlers.delete(handler);
    };
  }

  requirePgPool() {
    if (this.pgwire_pool == null) {
      throw new Error('No source connection configured');
    } else {
      return this.pgwire_pool!;
    }
  }
}
