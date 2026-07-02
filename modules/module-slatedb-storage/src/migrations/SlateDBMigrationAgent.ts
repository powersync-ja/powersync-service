import { framework, migrations } from '@powersync/service-core';

class SlateDBMigrationLockManager extends framework.locks.AbstractLockManager {
  private locked = false;

  protected async acquireHandle(): Promise<framework.locks.LockHandle | null> {
    if (this.locked) {
      return null;
    }
    this.locked = true;
    return {
      refresh: async () => {},
      release: async () => {
        this.locked = false;
      }
    };
  }
}

export class SlateDBMigrationAgent extends migrations.AbstractPowerSyncMigrationAgent {
  readonly locks = new SlateDBMigrationLockManager({ name: 'slatedb_migrations' });

  private state: framework.MigrationState | undefined;

  readonly store: framework.MigrationStore = {
    load: async () => this.state,
    save: async (state) => {
      this.state = state;
    },
    clear: async () => {
      this.state = undefined;
    }
  };

  getInternalScriptsDir(): string {
    throw new Error('SlateDB storage has no internal migrations.');
  }

  override async loadInternalMigrations(): Promise<framework.Migration<migrations.PowerSyncMigrationContext>[]> {
    return [];
  }

  async [Symbol.asyncDispose](): Promise<void> {
    this.state = undefined;
  }
}
