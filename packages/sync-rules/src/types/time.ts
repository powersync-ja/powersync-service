import { SqliteValueType } from '../ExpressionType.js';
import { CompatibilityContext, Quirk } from '../quirks.js';
import { CustomSqliteType } from './custom_sqlite_type.js';

/**
 * In old versions of the sync service, timestamp values were formatted with a space between the date and time
 * components.
 *
 * This is not ISO 6801 compatible, but changing it would be breaking existing users. So, this option is opt-in and
 * disabled by default until a major upgrade.
 */
export class TimeValue extends CustomSqliteType {
  // YYYY-MM-DD hh:mm:ss.sss / YYYY-MM-DD hh:mm:ss.sssZ
  readonly legacyRepresentation: string;
  // YYYY-MM-DDThh:mm:ss.sss / YYYY-MM-DDThh:mm:ss.sssZ
  readonly iso8601Representation: string;

  constructor(legacyRepresentation: string, iso8601Representation: string) {
    super();
    this.legacyRepresentation = legacyRepresentation;
    this.iso8601Representation = iso8601Representation;
  }

  get sqliteType(): SqliteValueType {
    return 'text';
  }

  toSqliteValue(context: CompatibilityContext) {
    return context.isFixed(Quirk.nonIso8601Timestampts) ? this.iso8601Representation : this.legacyRepresentation;
  }
}
