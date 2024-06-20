import winston from 'winston';

export namespace Logger {
  export const development_format = winston.format.combine(
    winston.format.colorize({ level: true }),
    winston.format.simple()
  );
  export const production_format = winston.format.combine(winston.format.timestamp(), winston.format.json());
}

export const logger = winston.createLogger();

// Configure logging to console as the default
logger.configure({
  format: process.env.NODE_ENV == 'production' ? Logger.production_format : Logger.development_format,
  transports: [new winston.transports.Console()]
});
