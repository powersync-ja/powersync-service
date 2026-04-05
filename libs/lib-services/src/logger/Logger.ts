import { ServiceAssertionError } from '@powersync/service-errors';

import jsonStringify from 'safe-stable-stringify';
import winston, { format } from 'winston';

const prefixFormat = winston.format((info) => {
  if (info.prefix) {
    info.message = `${info.prefix}${info.message}`;
  }
  return {
    ...info,
    prefix: undefined
  };
});

export const DEFAULT_LOG_LEVEL = 'info';
export const DEFAULT_LOG_FORMAT = process.env.NODE_ENV == 'production' ? 'json' : 'text';

/**
 * Set this field on an object to ensure it is never logged.
 * This will throw an assertion error if it is logged.
 */
export const DO_NOT_LOG = Symbol('DO_NOT_LOG');

/**
 * Filter potentially noise or sensitive values from logs.
 *
 * This throws a hard error if the DO_NOT_LOG Symbol is encountered.
 */
const logFilter = (key: string, value: unknown) => {
  if (value != null && typeof value == 'object' && (value as any)[DO_NOT_LOG]) {
    throw new ServiceAssertionError(`${Object.getPrototypeOf(value)?.constructor?.name} must not be logged`);
  }
  return value;
};

const MESSAGE = Symbol.for('message');

/**
 * Like winston.format.simple, but with a custom replacer to filter logs.
 */
const filteredSimple = format((info) => {
  const stringifiedRest = jsonStringify(
    Object.assign({}, info, {
      level: undefined,
      message: undefined,
      splat: undefined
    }),
    logFilter
  );

  const padding = (info.padding && info.padding[info.level]) || '';
  if (stringifiedRest !== '{}') {
    info[MESSAGE] = `${info.level}:${padding} ${info.message} ${stringifiedRest}`;
  } else {
    info[MESSAGE] = `${info.level}:${padding} ${info.message}`;
  }

  return info;
});

export namespace LogFormat {
  export const development = winston.format.combine(
    prefixFormat(),
    winston.format.colorize({ level: true }),
    filteredSimple()
  );
  export const production = winston.format.combine(
    prefixFormat(),
    winston.format.timestamp(),
    winston.format.json({ replacer: logFilter })
  );
}

export const logger = winston.createLogger();

// Set up default logging before config is loaded
logger.configure({
  level: DEFAULT_LOG_LEVEL,
  format: DEFAULT_LOG_FORMAT === 'json' ? LogFormat.production : LogFormat.development,
  transports: [new winston.transports.Console()]
});
