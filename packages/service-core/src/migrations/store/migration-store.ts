import { Db } from 'mongodb';
import * as path from 'path';
import * as defs from '../definitions.js';

export type MigrationStore = {
  load: () => Promise<defs.MigrationState | undefined>;
  save: (state: defs.MigrationState) => Promise<void>;
};

/**
 * A custom store for node-migrate which is used to save and load migrations that have
 * been operated on to mongo.
 */
export const createMongoMigrationStore = (db: Db): MigrationStore => {
  const collection = db.collection<defs.MigrationState>('migrations');

  return {
    load: async () => {
      const state_entry = await collection.findOne();
      if (!state_entry) {
        return;
      }

      const { _id, ...state } = state_entry;

      /**
       * This is for backwards compatibility. A previous version of the migration tool used to save
       * state as `lastRun`.
       */
      let last_run = state.last_run;
      if ('lastRun' in state) {
        last_run = (state as any).lastRun;
      }

      /**
       * This is for backwards compatibility. A previous version of the migration tool used to include the
       * file extension in migration names. This strips that extension off if it exists
       */
      const extension = path.extname(last_run);
      if (extension) {
        last_run = last_run.replace(extension, '');
      }

      return {
        last_run,
        log: state.log || []
      };
    },

    save: async (state: defs.MigrationState) => {
      await collection.replaceOne(
        {},
        {
          last_run: state.last_run,
          log: state.log
        },
        {
          upsert: true
        }
      );
    }
  };
};
