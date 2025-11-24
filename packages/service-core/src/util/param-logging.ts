export type ParamLoggingFormatOptions = {
  maxKeyCount: number;
  maxStringLength: number;
};

export const DEFAULT_PARAM_LOGGING_FORMAT_OPTIONS: ParamLoggingFormatOptions = {
  maxKeyCount: 10,
  maxStringLength: 50
};

export function formatParamsForLogging(params: Record<string, any>, options: Partial<ParamLoggingFormatOptions> = {}) {
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

      if (typeof value === 'string') {
        return [key, trimString(value)];
      }
      return [key, trimString(JSON.stringify(value))];
    })
  );
}
