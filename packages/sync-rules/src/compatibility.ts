export enum CompatibilityEdition {
  LEGACY = 1,
  SYNC_STREAMS = 2
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

  static byName: Record<string, CompatibilityOption> = Object.freeze({
    timestamps_iso8601: this.timestampsIso8601,
    versioned_bucket_ids: this.versionedBucketIds
  });
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

  constructor(edition: CompatibilityEdition, overrides?: Map<CompatibilityOption, boolean>) {
    this.edition = edition;
    this.overrides = overrides ?? new Map();
  }

  isEnabled(option: CompatibilityOption) {
    return this.overrides.get(option) ?? option.fixedIn <= this.edition;
  }

  /**
   * A {@link CompatibilityContext} in which no fixes are applied.
   */
  static FULL_BACKWARDS_COMPATIBILITY: CompatibilityContext = new CompatibilityContext(CompatibilityEdition.LEGACY);
}
