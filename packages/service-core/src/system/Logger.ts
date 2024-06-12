import winston from 'winston';

/**
 * Logger instance which is used in the entire codebase.
 * This should be configured in the project which consumes the
 * core package.
 */
export const logger = winston.createLogger();
