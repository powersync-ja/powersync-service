import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';

export const WriteCheckpoint = t.object({
  user_id: t.string,
  lsns: jsonb(t.record(t.string)),
  write_checkpoint: bigint
});

export type WriteCheckpoint = t.Encoded<typeof WriteCheckpoint>;
export type WriteCheckpointDecoded = t.Decoded<typeof WriteCheckpoint>;

export const CustomWriteCheckpoint = t.object({
  user_id: t.string,
  write_checkpoint: bigint,
  sync_rules_id: bigint
});

export type CustomWriteCheckpoint = t.Encoded<typeof CustomWriteCheckpoint>;
export type CustomWriteCheckpointDecoded = t.Decoded<typeof CustomWriteCheckpoint>;
