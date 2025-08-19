import { SqliteValueType } from '../ExpressionType.js';
import { CompatibilityContext, Quirk } from '../quirks.js';
import { SqliteValue } from '../types.js';
import { CustomSqliteValue } from './custom_sqlite_value.js';

/**
 * In old versions of the sync service, timestamp values were formatted with a space between the date and time
 * components.
 *
 * This is not ISO 6801 compatible, but changing it would be breaking existing users. So, this option is opt-in and
 * disabled by default until a major upgrade.
 */
export class DateTimeValue extends CustomSqliteValue {
  // YYYY-MM-DDThh:mm:ss.sss / YYYY-MM-DDThh:mm:ss.sssZ

  constructor(
    readonly iso8601Representation: string,
    private readonly fixedLegacyRepresentation: string | undefined = undefined
  ) {
    super();
  }

  // YYYY-MM-DD hh:mm:ss.sss / YYYY-MM-DD hh:mm:ss.sssZ
  public get legacyRepresentation(): string {
    return this.fixedLegacyRepresentation ?? this.iso8601Representation.replace('T', ' ');
  }

  get sqliteType(): SqliteValueType {
    return 'text';
  }

  toSqliteValue(context: CompatibilityContext) {
    return context.isFixed(Quirk.nonIso8601Timestampts) ? this.iso8601Representation : this.legacyRepresentation;
  }
}

/**
 * In old versions of the sync service, time values didn't consistently contain a sub-second interval.
 *
 * A value like `12:13:14.156789` would be represented as-is, but `12:13:14.000000` would be synced as `12:13:14`. This
 * is undesirable because it means that sorting values alphabetically doesn't preserve their value.
 */
export class TimeValue extends CustomSqliteValue {
  constructor(
    readonly timeSeconds: string,
    readonly fraction: string | undefined = undefined
  ) {
    super();
  }

  static parse(value: string): TimeValue | null {
    const match = /^([\d:]+)(\.\d+)?$/.exec(value);
    if (match == null) {
      return null;
    }

    const [_, timeSeconds, fraction] = match as any;
    return new TimeValue(timeSeconds, fraction);
  }

  toSqliteValue(context: CompatibilityContext): SqliteValue {
    if (context.isFixed(Quirk.nonIso8601Timestampts)) {
      const fraction = this.fraction?.padEnd(7, '0') ?? '.000000';
      return `${this.timeSeconds}${fraction}`;
    } else {
      return `${this.timeSeconds}${this.fraction ?? ''}`;
    }
  }

  get sqliteType(): SqliteValueType {
    return 'text';
  }
}
