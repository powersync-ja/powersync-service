import { schema } from '@powersync/lib-services-framework';
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

const CHECK_CONVEX_RESPONSES_ENV = 'POWERSYNC_DEV_CHECK_CONVEX_RESPONSES';

// These validators optionally assert the API response shape in development.
export const ensureConvexListSnapshotResult = ensureResponseFormatValidator(ConvexListSnapshotResult);

export const ConvexDocumentDeltasResult = t.object({
  cursor: bigint,
  hasMore: t.boolean,
  values: t.array(ConvexDocumentDelta)
});

export type ConvexDocumentDeltasResult = t.Encoded<typeof ConvexDocumentDeltasResult>;

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

export const RawJsonSchemaResponse = t.record(
  t.object({
    type: t.string,
    properties: t.record(t.any)
  })
);

export type RawJsonSchemaResponse = t.Encoded<typeof RawJsonSchemaResponse>;

export const ensureRawJsonSchemaResponse = ensureResponseFormatValidator(RawJsonSchemaResponse);

/**
 * Optionally validates that Convex API response data matches the codec specification.
 *
 * These checks were originally added because earlier iterations of this client
 * handled several Convex API route and field-name permutations, such as falling
 * back between `has_more` and `hasMore`. That suggested there may have been
 * response differences between Convex Cloud and self-hosted Convex deployments.
 *
 * The current response typings have been verified with cloud and self-hosted
 * integration tests, so we do not need to pay this validation cost in
 * production. The checks are still useful while developing against Convex API
 * changes, so they are kept as an opt-in guardrail.
 *
 * Set POWERSYNC_DEV_CHECK_CONVEX_RESPONSES to enable this development safety net.
 */
export function ensureResponseFormatValidator<Codec extends t.AnyCodec>(
  codec: Codec
): (data: unknown) => t.Encoded<Codec> {
  if (!process.env[CHECK_CONVEX_RESPONSES_ENV]) {
    return (data: unknown) => data as t.Encoded<Codec>;
  }

  const validator = schema.createTsCodecValidator(codec, {
    parsers: [bigintParser],
    allowAdditional: true
  });

  return (data: unknown) => {
    const result = validator.validate(data);
    if (!result.valid) {
      // This does not result in leaking failed data, it only logs the keys which failed validation
      throw new Error(
        `Invalid data received. Got parsing errors when checking data format. Errors: ${result.errors.join(', ')}`
      );
    }
    return data as t.Encoded<Codec>;
  };
}
