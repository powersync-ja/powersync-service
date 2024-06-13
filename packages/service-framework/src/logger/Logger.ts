import winston from 'winston';

/**
 * Logger instance which is used in the entire codebase.
 * This should be configured in the project which consumes the
 * core package.
 */
export const logger = winston.createLogger();

export namespace Logger {
  export const instance = logger;
  export const development_format = winston.format.combine(
    winston.format.colorize({ level: true }),
    winston.format.simple()
  );
  export const production_format = winston.format.combine(winston.format.timestamp(), winston.format.json());
}
