import { BufferNodeType } from '@powersync/lib-services-framework';
import AJV from 'ajv';
import * as t from 'ts-codec';

export const ConvexRawDocument = t
  .object({
    _id: t.string.optional(),
    _table: t.string.optional(),
    _deleted: t.boolean.optional()
  })
  .and(t.record(t.any));

export type ConvexRawDocument = t.Encoded<typeof ConvexRawDocument>;

// Placeholder to represent bigints
export const bigint = t.codec<bigint, bigint>(
  'bigint',
  (decoded: bigint) => decoded,
  (encoded: bigint) => encoded
);

// Placeholder which allows validating bigint payloads
export const bigintParser = t.createParser<typeof bigint>(bigint._tag, () => ({
  nodeType: 'bigint'
}));

export const ConvexDocumentDelta = ConvexRawDocument.and(
  t.object({
    _ts: bigint
  })
);

export type ConvexDocumentDelta = t.Encoded<typeof ConvexDocumentDelta>;

export const ConvexTableSchema = t.object({
  tableName: t.string,
  // JSON schema object
  schema: t.record(t.any)
});

export type ConvexTableSchema = t.Encoded<typeof ConvexTableSchema>;

export const ConvexJsonSchemasResult = t.object({
  tables: t.array(ConvexTableSchema)
});

export type ConvexJsonSchemasResult = t.Encoded<typeof ConvexJsonSchemasResult>;

export const ConvexListSnapshotResult = t.object({
  snapshot: bigint,
  cursor: t.string.or(t.Null),
  hasMore: t.boolean,
  values: t.array(ConvexRawDocument)
});

export type ConvexListSnapshotResult = t.Encoded<typeof ConvexListSnapshotResult>;

// These validators help assert the API structure response.
// These could be disabled for production.
export const ensureConvexListSnapshotResult = ensureResponseFormatValidator(ConvexListSnapshotResult);

export const ConvexDocumentDeltasResult = t.object({
  cursor: bigint,
  hasMore: t.boolean,
  values: t.array(ConvexDocumentDelta)
});

export type ConvexDocumentDeltasResult = t.Encoded<typeof ConvexDocumentDeltasResult>;

// These validators help assert the API structure response.
// These could be disabled for production.
export const ensureConvexDocumentDeltasResult = ensureResponseFormatValidator(ConvexDocumentDeltasResult);

export interface ConvexListSnapshotOptions {
  snapshot?: string;
  cursor?: string;
  tableName?: string;
  signal?: AbortSignal;
}

export interface ConvexDocumentDeltasOptions {
  cursor?: string;
  signal?: AbortSignal;
}

/**
 * Performs a validation which ensures the input data matches the codec specification.
 */
export function ensureResponseFormatValidator<Codec extends t.AnyCodec>(
  codec: Codec
): (data: unknown) => t.Encoded<Codec> {
  const schema = t.generateJSONSchema(codec, {
    parsers: [bigintParser],
    allowAdditional: true
  });
  const ajv = new AJV.Ajv({
    allErrors: true,
    keywords: [BufferNodeType]
  });

  const validator = ajv.compile(schema);
  return (data: unknown) => {
    const isValid = validator(data);
    if (!isValid) {
      // This does not result in leaking failed data, it only logs the keys which failed validation
      throw new Error(
        `Invalid data received. Got parsing errors when checking data format. Keys which failed validation: ${ajv.errors?.map((e) => e.propertyName).join(', ')}`
      );
    }
    return data as t.Encoded<Codec>;
  };
}
