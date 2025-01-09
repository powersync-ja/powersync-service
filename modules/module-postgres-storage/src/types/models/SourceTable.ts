import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';

export const ColumnDescriptor = t.object({
  name: t.string,
  /**
   *  The type of the column ie VARCHAR, INT, etc
   */
  type: t.string.optional(),
  /**
   *  Some data sources have a type id that can be used to identify the type of the column
   */
  typeId: t.number.optional()
});

export const SourceTable = t.object({
  id: t.string,
  group_id: bigint,
  connection_id: bigint,
  relation_id: t.Null.or(t.number).or(t.string),
  schema_name: t.string,
  table_name: t.string,
  replica_id_columns: t.Null.or(jsonb(t.array(ColumnDescriptor))),
  snapshot_done: t.boolean
});

export type SourceTable = t.Encoded<typeof SourceTable>;
export type SourceTableDecoded = t.Decoded<typeof SourceTable>;
