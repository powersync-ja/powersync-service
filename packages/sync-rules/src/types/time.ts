import { SqliteValueType } from '../ExpressionType.js';
import { CompatibilityContext, Quirk } from '../quirks.js';
import { CustomSqliteValue } from './custom_sqlite_value.js';

/**
 * In old versions of the sync service, timestamp values were formatted with a space between the date and time
 * components.
 *
 * This is not ISO 6801 compatible, but changing it would be breaking existing users. So, this option is opt-in and
 * disabled by default until a major upgrade.
 */
export class TimeValue extends CustomSqliteValue {
  // YYYY-MM-DDThh:mm:ss.sss / YYYY-MM-DDThh:mm:ss.sssZ
  readonly iso8601Representation: string;

  constructor(iso8601Representation: string) {
    super();
    this.iso8601Representation = iso8601Representation;
  }

  // YYYY-MM-DD hh:mm:ss.sss / YYYY-MM-DD hh:mm:ss.sssZ
  public get legacyRepresentation(): string {
    return this.iso8601Representation.replace('T', ' ');
  }

  get sqliteType(): SqliteValueType {
    return 'text';
  }

  toSqliteValue(context: CompatibilityContext) {
    return context.isFixed(Quirk.nonIso8601Timestampts) ? this.iso8601Representation : this.legacyRepresentation;
  }
}
