import { framework } from '@powersync/service-core';
import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';

export const CurrentBucket = t.object({
  bucket: t.string,
  table: t.string,
  id: t.string
});

export type CurrentBucket = t.Encoded<typeof CurrentBucket>;
export type CurrentBucketDecoded = t.Decoded<typeof CurrentBucket>;

export const CurrentData = t.object({
  buckets: jsonb(t.array(CurrentBucket)),
  data: framework.codecs.buffer,
  group_id: bigint,
  lookups: t.array(framework.codecs.buffer),
  source_key: framework.codecs.buffer,
  source_table: t.string
});

export type CurrentData = t.Encoded<typeof CurrentData>;
export type CurrentDataDecoded = t.Decoded<typeof CurrentData>;
