export enum CompatibilityLevel {
  LEGACY = 0,
  SYNC_STREAMS = 1
}

/**
 * A historical issue of the PowerSync service that can only be changed in a backwards-incompatible manner.
 *
 * To avoid breaking existing users, fixes to those quirks are opt-in: Users either have to use `fixed_quirks` list when
 * defining sync rules or use a new feature such as sync streams where these issues are fixed by default.
 */
export class Quirk {
  readonly name: string;
  readonly description: string;
  readonly fixedIn: CompatibilityLevel;

  private constructor(name: string, description: string, fixedIn: CompatibilityLevel) {
    this.name = name;
    this.description = description;
    this.fixedIn = fixedIn;
  }

  static nonIso8601Timestampts = new Quirk(
    'non_iso8601_timestamps',
    'Old versions of the sync service used to encode timestamps as `YYYY-MM-DD hh:mm:ss.sss`. With this quirk fixed, `YYYY-MM-DDThh:mm:ss.sssZ` is used instead.',
    CompatibilityLevel.SYNC_STREAMS
  );

  static byName: Record<string, Quirk> = (() => {
    const byName: Record<string, Quirk> = {};
    for (const entry of [this.nonIso8601Timestampts]) {
      byName[entry.name] = entry;
    }

    return byName;
  })();
}

export class CompatibilityContext {
  /**
   * The general compatibility level we're operating under.
   *
   * This is {@link CompatibilityLevel.LEGACY} by default, but can be changed in contexts that implicitly opt-in to
   * fixed quirks.
   */
  readonly level: CompatibilityLevel;

  /**
   * Quirks for which the user has opted in to fixes explicitly.
   */
  readonly explicitlyFixed: Quirk[];

  constructor(level: CompatibilityLevel, explicitlyFixed: Quirk[] = []) {
    this.level = level;
    this.explicitlyFixed = explicitlyFixed;
  }

  isFixed(quirk: Quirk) {
    return quirk.fixedIn <= this.level || this.explicitlyFixed.indexOf(quirk) != -1;
  }

  /**
   * Creates a compatibility context with the {@link CompatibilityLevel.LEGACY} level and an array of fixes to apply.
   */
  static ofFixedQuirks(fixed: Quirk[]) {
    return new CompatibilityContext(CompatibilityLevel.LEGACY, fixed);
  }

  /**
   * A {@link CompatibilityContext} in which no fixes are applied.
   */
  static FULL_BACKWARDS_COMPATIBILITY: CompatibilityContext = new CompatibilityContext(CompatibilityLevel.LEGACY, []);
}
