import * as t from 'ts-codec';
import { bigint, jsonb } from '../codecs.js';

export const SdkReporting = t.object({
  users: bigint,
  sdks: t.object({
    data: jsonb(t.record(t.string))
  })
});

export type SdkReporting = t.Encoded<typeof SdkReporting>;
