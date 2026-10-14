import type { ErrorLocation } from '../errors.js';
import type { SyncConfigSourceLocationTarget, SyncConfigSourcePath } from '../SyncConfigDiagnostics.js';

/**
 * A path into the authored expression, e.g. `['expression', '$eq', 1, '$oid']`, without configured literal values.
 */
export class MongoFilterValidationError extends Error {
  readonly location?: ErrorLocation;
  readonly detail: string;
  readonly path: SyncConfigSourcePath;
  readonly target: SyncConfigSourceLocationTarget;

  /**
   * Formats the diagnostic path and retains its key/value target for YAML source highlighting.
   */
  constructor({
    path,
    message,
    target = 'value',
    location
  }: {
    path: SyncConfigSourcePath;
    message: string;
    target?: SyncConfigSourceLocationTarget;
    location?: ErrorLocation;
  }) {
    const formattedPath = path
      .map((part, index) => (typeof part == 'number' ? `[${part}]` : `${index ? '.' : ''}${part}`))
      .join('');
    super(`Invalid MongoDB pre-filtering expression at '${formattedPath}': ${message}`);
    this.name = 'MongoFilterValidationError';
    this.detail = message;
    this.location = location;
    this.path = path;
    this.target = target;
  }
}

/**
 * Supported single-key Extended JSON wrappers. Payload validation belongs to the individual codecs.
 */
export const SUPPORTED_EXTENDED_JSON_WRAPPERS: ReadonlySet<string> = new Set([
  '$oid',
  '$numberInt',
  '$numberLong',
  '$numberDouble',
  '$numberDecimal',
  '$date',
  '$timestamp',
  '$binary',
  '$uuid'
]);

/**
 * Authored documents are plain JSON objects; native Date/BSON instances must use their EJSON spelling.
 */
export function isMongoDocument(value: unknown): value is Record<string, unknown> {
  if (value == null || typeof value != 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
