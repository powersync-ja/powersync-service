import { Decimal128, EJSON } from 'bson';
import { MongoFilterValidationError } from './MongoFilterValidation.js';

/**
 * Reports a payload error relative to the codec node; Zod supplies the surrounding source path.
 */
function fail(message: string): never {
  throw new MongoFilterValidationError({ path: [], message });
}

/**
 * Checks integer-string bounds without losing Int64 precision. Syntax is constrained by the codec's pattern.
 */
function integerRange(value: unknown, minimum: bigint, maximum: bigint): void {
  if (typeof value != 'string' || !/^-?(?:0|[1-9][0-9]*)$/.test(value)) return;
  const parsed = BigInt(value);
  if (parsed < minimum || parsed > maximum) fail('integer is outside the supported BSON range.');
}

/**
 * Specialized payload checks attached directly to Zod schemas. Wrapper selection and recursion belong to the schema.
 */
export const MONGO_BSON_VALIDATORS = {
  mongoNumber: {
    type: 'number',
    /**
     * Plain integer literals have an Int32 interpretation; larger integers must select an EJSON type explicitly.
     */
    validate(value: unknown) {
      if (typeof value == 'number' && Number.isInteger(value) && (value < -2_147_483_648 || value > 2_147_483_647)) {
        fail('integers outside the BSON Int32 range require an EJSON wrapper.');
      }
    }
  },
  mongoInt32: {
    type: 'string',
    /**
     * Checks a canonical Int32 payload against its signed bounds.
     */
    validate(value: unknown) {
      integerRange(value, -2_147_483_648n, 2_147_483_647n);
    }
  },
  mongoInt64: {
    type: 'string',
    /**
     * Checks a canonical Int64 payload using exact integer arithmetic.
     */
    validate(value: unknown) {
      integerRange(value, -9_223_372_036_854_775_808n, 9_223_372_036_854_775_807n);
    }
  },
  mongoDouble: {
    type: 'string',
    /**
     * Rejects overflow to infinity after the schema checks decimal/exponent syntax.
     */
    validate(value: unknown) {
      if (!Number.isFinite(Number(value))) fail('expected a finite Extended JSON double string.');
    }
  },
  mongoDecimal128: {
    type: 'string',
    /**
     * Delegates representability to BSON rather than converting through a lossy JavaScript number.
     */
    validate(value: unknown) {
      if (typeof value != 'string' || /nan|inf/i.test(value))
        fail('expected a finite Extended JSON Decimal128 string.');
      try {
        Decimal128.fromString(value);
      } catch {
        fail('expected a valid Extended JSON Decimal128 string.');
      }
    }
  },
  mongoDateString: {
    type: 'string',
    /**
     * Checks date validity after the schema requires an ISO timestamp with an explicit timezone.
     */
    validate(value: unknown) {
      if (typeof value != 'string' || !Number.isFinite(Date.parse(value))) fail('expected a valid ISO date string.');
    }
  },
  mongoDateMillis: {
    type: 'string',
    /**
     * BSON dates are restored as JavaScript Date objects and must fit their supported millisecond range.
     */
    validate(value: unknown) {
      integerRange(value, -8_640_000_000_000_000n, 8_640_000_000_000_000n);
    }
  },
  mongoBinary: {
    type: 'object',
    /**
     * Checks BSON subtype constraints, including UUID byte length, after structural byte-encoding validation.
     */
    validate(value: unknown) {
      try {
        EJSON.deserialize({ $binary: value }, { relaxed: false });
      } catch {
        fail('the Extended JSON binary value is invalid or unsupported.');
      }
    }
  }
} as const;
