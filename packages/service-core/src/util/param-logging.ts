/**
 * Options for {@link limitParamsForLogging}.
 */
export type ParamLoggingFormatOptions = {
  maxKeyCount: number;
  maxStringLength: number;
};

/**
 * Default options for {@link limitParamsForLogging}.
 */
export const DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS: ParamLoggingFormatOptions = {
  maxKeyCount: 20,
  maxStringLength: 100
};

/**
 * Formats potentially arbitrary parameters for logging.
 * This limits the number of keys and strings to a maximum length.
 * A warning key-value is added if the number of keys exceeds the maximum.
 * String values exceeding the maximum length are truncated.
 * Non-String values are stringified, the maximum length is then applied.
 * @param params - The parameters to format.
 * @param options - The options to use.
 * @default DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS
 * @returns The formatted parameters.
 */
export function limitParamsForLogging(
  params: Record<string, any>,
  options: Partial<ParamLoggingFormatOptions> = DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS
) {
  const {
    maxStringLength = DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS.maxStringLength,
    maxKeyCount = DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS.maxKeyCount
  } = options;

  function trimString(value: string): string {
    if (value.length > maxStringLength) {
      return value.slice(0, maxStringLength - 3) + '...';
    }
    return value;
  }

  return Object.fromEntries(
    Object.entries(params).map(([key, value], index) => {
      if (index == maxKeyCount) {
        return ['⚠️', 'Additional parameters omitted'];
      }

      if (index > maxKeyCount) {
        return [];
      }

      if (typeof value == 'string') {
        return [key, trimString(value)];
      }
      return [key, trimString(JSON.stringify(value))];
    })
  );
}
