import winston from 'winston';

export namespace Logger {
  export const development_format = winston.format.combine(
    winston.format.colorize({ level: true }),
    winston.format.simple()
  );
  export const production_format = winston.format.combine(winston.format.timestamp(), winston.format.json());
}
