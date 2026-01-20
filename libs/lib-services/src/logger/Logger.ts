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

export namespace LogFormat {
  export const development = winston.format.combine(
    prefixFormat(),
    winston.format.colorize({ level: true }),
    winston.format.simple()
  );
  export const production = winston.format.combine(prefixFormat(), winston.format.timestamp(), winston.format.json());
}

const LOG_LEVEL = process.env.PS_LOG_LEVEL ?? 'info';

export const logger = winston.createLogger();

// Configure logging to console as the default
logger.configure({
  level: LOG_LEVEL,
  format: process.env.NODE_ENV == 'production' ? LogFormat.production : LogFormat.development,
  transports: [new winston.transports.Console()]
});
