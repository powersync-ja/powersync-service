import { SqliteValueType } from '../ExpressionType.js';
import { CompatibilityContext, CompatibilityOption, TimeValuePrecision } from '../compatibility.js';
import { SqliteValue } from '../types.js';
import { CustomSqliteValue } from './custom_sqlite_value.js';

export interface DateTimeSourceOptions {
  /**
   * The amount of sub-second digits provided by the source database.
   *
   * We can't infer this by parsing the iso representation alone, since trailing zeroes might be omitted from the
   * representation.
   */
  subSecondPrecision: TimeValuePrecision;
  /**
   * The default precision to use when rendering {@link DateTimeValue}s in a compatibility context that doesn't have a
   * {@link CompatibilityContext.maxTimeValuePrecision} set.
   *
   * This is usually the same as {@link subSecondPrecision}, except for MySQL, where we can provide microseconds
   * precision but default to milliseconds.
   */
  defaultSubSecondPrecision: TimeValuePrecision;
}

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
    private readonly fixedLegacyRepresentation: string | undefined = undefined,
    private readonly options: DateTimeSourceOptions
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
    if (context.isEnabled(CompatibilityOption.timestampsIso8601)) {
      return renderSubseconds(
        () => {
          // Match the `.123` subsecond part and/or a `Z` suffix.
          const matchSubSeconds = /(?:\.(\d+))?([zZ]?)$/.exec(this.iso8601Representation);
          if (matchSubSeconds == null || matchSubSeconds[0].length == 0) {
            return [this.iso8601Representation, '', ''];
          } else {
            return [
              this.iso8601Representation.slice(0, -matchSubSeconds[0].length),
              matchSubSeconds[1] ?? '',
              matchSubSeconds[2] ?? ''
            ];
          }
        },
        this.options,
        context
      );
    } else {
      return this.legacyRepresentation;
    }
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
    readonly fraction: string,
    private readonly options: DateTimeSourceOptions
  ) {
    super();
  }

  static parse(value: string, options: DateTimeSourceOptions): TimeValue | null {
    const match = /^([\d:]+)(?:\.(\d+))?$/.exec(value);
    if (match == null) {
      return null;
    }

    const [_, timeSeconds, fraction] = match as any;
    return new TimeValue(timeSeconds, fraction, options);
  }

  toSqliteValue(context: CompatibilityContext): SqliteValue {
    if (context.isEnabled(CompatibilityOption.timestampsIso8601)) {
      return renderSubseconds(() => [this.timeSeconds, this.fraction ?? '', ''], this.options, context);
    } else {
      if (this.fraction) {
        return `${this.timeSeconds}.${this.fraction}`;
      } else {
        return this.timeSeconds;
      }
    }
  }

  get sqliteType(): SqliteValueType {
    return 'text';
  }
}

/**
 * Renders a time value with a designated precision.
 *
 * @param split Returns the original time value, split into the part before the sub-second fraction and the subsecond
 * fraction. The `.` separator should not be part of either string.
 * @param options Information about precision overred by the source database.
 * @param context The {@link CompatibilityContext} to take into consideration.
 * @returns The rendered value.
 */
function renderSubseconds(
  split: () => [string, string, string],
  options: DateTimeSourceOptions,
  context: CompatibilityContext
) {
  const maxPrecision = context.maxTimeValuePrecision ?? options.defaultSubSecondPrecision;
  const maxSubSecondDigits = Math.min(maxPrecision.subSecondDigits, options.subSecondPrecision.subSecondDigits);

  // Note: We are deliberately not rounding here, we always trim precision away. Rounding would require a parsed date
  // with subsecond precision, which is just painful in JS. Maybe with the temporal API in Node 25...
  let [withoutSubseconds, subseconds, suffix] = split();
  if (maxPrecision == TimeValuePrecision.seconds) {
    // Avoid a trailing `.` if we only care about seconds.
    return `${withoutSubseconds}${suffix}`;
  }

  if (subseconds.length > maxSubSecondDigits) {
    // Trim unwanted precision.
    subseconds = subseconds.substring(0, maxSubSecondDigits);
  } else if (subseconds.length < maxSubSecondDigits) {
    // Let's say we had a source database stripping trailing zeroes from the subsecond field. Perhaps the
    // subSecondPrecision is generally micros, but one value has .123456 and one has .1234 instead of .123400. For
    // consistency, we pad those value.
    subseconds = subseconds.padEnd(maxSubSecondDigits, '0');
  }

  return `${withoutSubseconds}.${subseconds}${suffix}`;
}
