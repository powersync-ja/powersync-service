import winston from 'winston';

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

export namespace LogFormat {
  export const development = winston.format.combine(
    prefixFormat(),
    winston.format.colorize({ level: true }),
    winston.format.simple()
  );
  export const production = winston.format.combine(prefixFormat(), winston.format.timestamp(), winston.format.json());
}

export const logger = winston.createLogger();

// Set up default logging before config is loaded
logger.configure({
  level: DEFAULT_LOG_LEVEL,
  format: DEFAULT_LOG_FORMAT === 'json' ? LogFormat.production : LogFormat.development,
  transports: [new winston.transports.Console()]
});
