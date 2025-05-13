import winston from 'winston';

export namespace LogFormat {
  export const development = winston.format.combine(winston.format.colorize({ level: true }), winston.format.simple());
  export const production = winston.format.combine(winston.format.timestamp(), winston.format.json());
}

export const logger = winston.createLogger();

// Configure logging to console as the default
logger.configure({
  format: process.env.NODE_ENV == 'production' ? LogFormat.production : LogFormat.development,
  transports: [new winston.transports.Console()]
});
