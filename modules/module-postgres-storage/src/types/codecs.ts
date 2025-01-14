import * as t from 'ts-codec';

export const BIGINT_MAX = BigInt('9223372036854775807');

/**
 * The use of ts-codec:
 * We currently use pgwire for Postgres queries. This library provides fine-grained control
 * over parameter typings and efficient streaming of query responses. Additionally, configuring
 * pgwire with default certificates allows us to use the same connection configuration process
 * for both replication and storage libraries.
 *
 * Unfortunately, ORM driver support for pgwire is limited, so we rely on pure SQL queries in the
 * absence of writing an ORM driver from scratch.
 *
 * [Opinion]: Writing pure SQL queries throughout a codebase can be daunting from a maintenance
 * and debugging perspective. For example, row response types are often declared when performing a query:
 *
 * ```typescript
 * const rows = await db.queryRows<MyRowType>(`SELECT one, two FROM my_table`);
 * ```
 * This type declaration suggests `rows` is an array of `MyRowType` objects, even though no validation
 * is enforced. Adding a field to the `MyRowType` interface without updating the query could easily
 * introduce subtle bugs. Similarly, type mismatches between SQL results and TypeScript interfaces, such as
 * a `Date` field returned as a `string`, require manual conversion.
 *
 * `ts-codec` is not an ORM, but it simplifies working with pure SQL query responses in several ways:
 *
 * - **Validations**: The `decode` operation ensures that the returned row matches the expected object
 *   structure, throwing an error if it doesn't.
 * - **Decoding Columns**: pgwire already decodes common SQLite types, but `ts-codec` adds an extra layer
 *   for JS-native values. For instance, `jsonb` columns are returned as `JsonContainer`/`string` and can
 *   be automatically parsed into objects. Similarly, fields like `group_id` are converted from `Bigint`
 *   to `Number` for easier use.
 * - **Encoded Forms**: A single `ts-codec` type definition can infer both encoded and decoded forms. This
 *   is especially useful for persisted batch operations that rely on JSON query parameters for bulk inserts.
 *   Collections like `bucket_data`, `current_data`, and `bucket_parameters` use encoded/decoded types, making
 *   changes easier to manage and validate. While some manual encoding is done for intermediate values (e.g.,
 *   size estimation), these types are validated with `ts-codec` to ensure consistency.
 */

/**
 * Wraps a codec which is encoded to a JSON string
 */
export const jsonb = <Decoded>(subCodec: t.Codec<Decoded, any>) =>
  t.codec<Decoded, string>(
    'jsonb',
    (decoded: Decoded) => {
      return JSON.stringify(subCodec.encode(decoded) as any);
    },
    (encoded: string | { data: string }) => {
      const s = typeof encoded == 'object' ? encoded.data : encoded;
      return subCodec.decode(JSON.parse(s));
    }
  );

export const bigint = t.codec<bigint, string | number>(
  'bigint',
  (decoded: BigInt) => {
    return decoded.toString();
  },
  (encoded: string | number) => {
    return BigInt(encoded);
  }
);

export const uint8array = t.codec<Uint8Array, Uint8Array>(
  'uint8array',
  (d) => d,
  (e) => e
);

/**
 * PGWire returns BYTEA values as Uint8Array instances.
 * We also serialize to a hex string for bulk inserts.
 */
export const hexBuffer = t.codec(
  'hexBuffer',
  (decoded: Buffer) => {
    return decoded.toString('hex');
  },
  (encoded: string | Uint8Array) => {
    if (encoded instanceof Uint8Array) {
      return Buffer.from(encoded);
    }
    if (typeof encoded !== 'string') {
      throw new Error(`Expected either a Buffer instance or hex encoded buffer string`);
    }
    return Buffer.from(encoded, 'hex');
  }
);

/**
 * PGWire returns INTEGER columns as a `bigint`.
 * This does a decode operation to `number`.
 */
export const pgwire_number = t.codec(
  'pg_number',
  (decoded: number) => decoded,
  (encoded: bigint | number) => {
    if (typeof encoded == 'number') {
      return encoded;
    }
    if (typeof encoded !== 'bigint') {
      throw new Error(`Expected either number or bigint for value`);
    }
    if (encoded > BigInt(Number.MAX_SAFE_INTEGER) || encoded < BigInt(Number.MIN_SAFE_INTEGER)) {
      throw new RangeError('BigInt value is out of safe integer range for conversion to Number.');
    }
    return Number(encoded);
  }
);
