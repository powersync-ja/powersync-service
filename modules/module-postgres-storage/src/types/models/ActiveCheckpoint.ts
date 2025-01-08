import * as t from 'ts-codec';
import { bigint } from '../codecs.js';

/**
 * Notification payload sent via Postgres' NOTIFY API.
 *
 */
export const ActiveCheckpoint = t.object({
  id: bigint,
  last_checkpoint: t.Null.or(bigint),
  last_checkpoint_lsn: t.Null.or(t.string)
});

export type ActiveCheckpoint = t.Encoded<typeof ActiveCheckpoint>;
export type ActiveCheckpointDecoded = t.Decoded<typeof ActiveCheckpoint>;
