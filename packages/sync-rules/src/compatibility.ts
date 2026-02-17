export enum CompatibilityEdition {
  LEGACY = 1,
  SYNC_STREAMS = 2
}

export class TimeValuePrecision {
  private constructor(
    readonly name: string,
    readonly subSecondDigits: number
  ) {}

  static seconds = new TimeValuePrecision('seconds', 0);
  static milliseconds = new TimeValuePrecision('milliseconds', 3);
  static microseconds = new TimeValuePrecision('microseconds', 6);
  static nanoseconds = new TimeValuePrecision('nanoseconds', 9);

  static byName: Record<string, TimeValuePrecision> = Object.freeze({
    seconds: this.seconds,
    milliseconds: this.milliseconds,
    microseconds: this.microseconds,
    nanoseconds: this.nanoseconds
  });
}

/**
 * A historical issue of the PowerSync service that can only be changed in a backwards-incompatible manner.
 *
 * To avoid breaking existing users, fixes to those quirks are opt-in: Users either have to use `fixed_quirks` list when
 * defining sync rules or use a new feature such as sync streams where these issues are fixed by default.
 */
export class CompatibilityOption {
  private constructor(
    readonly name: string,
    readonly description: string,
    readonly fixedIn: CompatibilityEdition
  ) {}

  static timestampsIso8601 = new CompatibilityOption(
    'timestamps_iso8601',
    'Consistently renders timestamps with an ISO 8601-compatible format (previous versions used a space instead of a T to separate date and time).',
    CompatibilityEdition.SYNC_STREAMS
  );

  static versionedBucketIds = new CompatibilityOption(
    'versioned_bucket_ids',
    'Encode the version of sync rules in generated bucket ids, which avoids clients downloading data twice and improves client-side progress estimates.',
    CompatibilityEdition.SYNC_STREAMS
  );

  static fixedJsonExtract = new CompatibilityOption(
    'fixed_json_extract',
    "Old versions of the sync service used to treat `->> 'foo.bar'` as a two-element JSON path. With this compatibility option enabled, it follows modern SQLite and treats it as a single key. The `$.` prefix would be required to split on `.`.",
    CompatibilityEdition.SYNC_STREAMS
  );

  static customTypes = new CompatibilityOption(
    'custom_postgres_types',
    'Map custom Postgres types into appropriate structures instead of syncing the raw string.',
    CompatibilityEdition.SYNC_STREAMS
  );

  static byName: Record<string, CompatibilityOption> = Object.freeze({
    timestamps_iso8601: this.timestampsIso8601,
    versioned_bucket_ids: this.versionedBucketIds,
    fixed_json_extract: this.fixedJsonExtract,
    custom_postgres_types: this.customTypes
  });
}

export interface CompatibilityContextOptions {
  edition: CompatibilityEdition;
  overrides?: Map<CompatibilityOption, boolean>;
  maxTimeValuePrecision?: TimeValuePrecision;
}

export class CompatibilityContext {
  /**
   * The general compatibility level we're operating under.
   *
   * This is {@link CompatibilityEdition.LEGACY} by default, but can be changed when defining sync rules to allow newer
   * features.
   */
  readonly edition: CompatibilityEdition;

  /**
   * Overrides to customize used compatibility options to deviate from defaults at the given {@link edition}.
   */
  readonly overrides: Map<CompatibilityOption, boolean>;

  /**
   * A limit for the precision the sync service uses to encode time values.
   *
   * This can be set to e.g. `milliseconds` to only emit three digits of sub-second precision even if the source
   * database uses microseconds natively.
   */
  readonly maxTimeValuePrecision: TimeValuePrecision | null;

  constructor(options: CompatibilityContextOptions) {
    this.edition = options.edition;
    this.overrides = options.overrides ?? new Map();
    this.maxTimeValuePrecision = options.maxTimeValuePrecision ?? null;
  }

  isEnabled(option: CompatibilityOption) {
    return this.overrides.get(option) ?? option.fixedIn <= this.edition;
  }

  serialize(): SerializedCompatibilityContext {
    const serialized: SerializedCompatibilityContext = {
      edition: this.edition,
      overrides: {}
    };

    this.overrides.forEach((enabled, key) => (serialized.overrides[key.name] = enabled));
    if (this.maxTimeValuePrecision) {
      serialized.maxTimeValuePrecision = this.maxTimeValuePrecision.subSecondDigits;
    }

    return serialized;
  }

  static deserialize(serialized: SerializedCompatibilityContext): CompatibilityContext {
    const overrides = new Map<CompatibilityOption, boolean>();
    for (const [option, enabled] of Object.entries(serialized.overrides)) {
      const knownOption = CompatibilityOption.byName[option];
      if (knownOption) {
        overrides.set(knownOption, enabled);
      }
    }

    let maxTimeValuePrecision: TimeValuePrecision | undefined;
    if (serialized.maxTimeValuePrecision != null) {
      for (const option of Object.values(TimeValuePrecision.byName)) {
        if (option.subSecondDigits == serialized.maxTimeValuePrecision) {
          maxTimeValuePrecision = option;
          break;
        }
      }
    }

    return new CompatibilityContext({
      edition: serialized.edition,
      overrides,
      maxTimeValuePrecision
    });
  }

  /**
   * A {@link CompatibilityContext} in which no fixes are applied.
   */
  static FULL_BACKWARDS_COMPATIBILITY: CompatibilityContext = new CompatibilityContext({
    edition: CompatibilityEdition.LEGACY
  });
}

export interface SerializedCompatibilityContext {
  edition: number;
  overrides: Record<string, boolean>;
  maxTimeValuePrecision?: number;
}
