import * as t from 'ts-codec';

export const Instance = t.object({
  id: t.string
});

export type Instance = t.Encoded<typeof Instance>;
export type InstanceDecoded = t.Decoded<typeof Instance>;
