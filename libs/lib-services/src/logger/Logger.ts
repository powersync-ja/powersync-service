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

const DEFAULT_LOG_LEVEL = 'info';
const DEFAULT_LOG_FORMAT = process.env.NODE_ENV == 'production' ? 'json' : 'text';

// This interface mirrors the logging config in @powersync/service-types PowerSyncConfig.ts
// Keep in sync if modifying
interface LoggingConfig {
  level?: string;
  format?: 'json' | 'text';
}

export const logger = winston.createLogger();

export function configureLogger(config?: LoggingConfig): void {
  const level = process.env.PS_LOG_LEVEL ?? config?.level ?? DEFAULT_LOG_LEVEL;
  const format = (process.env.PS_LOG_FORMAT as LoggingConfig['format']) ?? config?.format ?? DEFAULT_LOG_FORMAT;
  const winstonFormat =
    format === 'json'
      ? winston.format.combine(prefixFormat(), winston.format.timestamp(), winston.format.json())
      : winston.format.combine(prefixFormat(), winston.format.colorize({ level: true }), winston.format.simple());

  logger.configure({ level, format: winstonFormat, transports: [new winston.transports.Console()] });
  logger.info(`Configured logger with level "${level}" and format "${format}"`);
}

// Set up default logging before config is loaded
configureLogger();