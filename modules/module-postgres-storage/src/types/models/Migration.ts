import { framework } from '@powersync/service-core';
import * as t from 'ts-codec';
import { jsonb } from '../codecs.js';

export const Migration = t.object({
  last_run: t.string,
  log: jsonb(
    t.array(
      t.object({
        name: t.string,
        direction: t.Enum(framework.migrations.Direction),
        timestamp: framework.codecs.date
      })
    )
  )
});

export type Migration = t.Encoded<typeof Migration>;
export type MigrationDecoded = t.Decoded<typeof Migration>;
