import { ErrorCode } from './codes.js';

export enum ErrorSeverity {
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error'
}

export type ErrorData = {
  name?: string;

  code: ErrorCode;
  description: string;

  severity?: ErrorSeverity;
  details?: string;
  status?: number;
  stack?: string;

  origin?: string;

  trace_id?: string;
};

export class ServiceError extends Error {
  is_service_error = true;

  errorData: ErrorData;

  static isServiceError(input: any): input is ServiceError {
    return input instanceof ServiceError || input?.is_service_error == true;
  }

  private static errorMessage(data: ErrorData | ErrorCode, description?: string) {
    if (typeof data == 'string') {
      data = {
        code: data,
        description: description!
      };
    }
    let message = `[${data.code}] ${data.description}`;
    if (data.details) {
      message += `\n  ${data.details}`;
    }
    return message;
  }

  constructor(data: ErrorData);
  constructor(code: ErrorCode, description: string);

  constructor(data: ErrorData | ErrorCode, description?: string) {
    super(ServiceError.errorMessage(data, description));
    if (typeof data == 'string') {
      data = {
        code: data,
        description: description!
      };
    }
    this.errorData = data;
    if (data.stack) {
      this.stack = data.stack;
    }

    this.name = data.name || this.constructor.name;
    this.errorData.name = this.name;
  }

  toString() {
    return this.stack;
  }

  toJSON(): ErrorData {
    if (process.env.NODE_ENV !== 'production') {
      return this.errorData;
    }
    return {
      name: this.errorData.name,
      code: this.errorData.code,
      status: this.errorData.status,
      description: this.errorData.description,
      details: this.errorData.details,
      trace_id: this.errorData.trace_id,
      severity: this.errorData.severity,
      origin: this.errorData.origin
    };
  }

  setTraceId(id: string) {
    this.errorData.trace_id = id;
  }
}

/**
 * @deprecated Use more specific errors
 */
export class ValidationError extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S2001;

  constructor(errors: any) {
    super({
      code: ValidationError.CODE,
      status: 400,
      description: 'Validation failed',
      details: JSON.stringify(errors)
    });
  }
}

/**
 * Use for replication errors that are never expected to happen in production.
 *
 * If it does happen, it is either:
 * 1. A bug in the code that should be fixed.
 * 2. An error that needs a different error code.
 */
export class ReplicationAssertionError extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S1101;
  constructor(description: string) {
    super({
      code: ReplicationAssertionError.CODE,
      status: 500,
      description: description
    });
  }
}

/**
 * Use for general service errors that are never expected to happen in production.
 *
 * If it does happen, it is either:
 * 1. A bug in the code that should be fixed.
 * 2. An error that needs a different error code.
 */
export class ServiceAssertionError extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S0001;
  constructor(description: string) {
    super({
      code: ServiceAssertionError.CODE,
      status: 500,
      description: description
    });
  }
}

/**
 * Indicates replication is aborted.
 *
 * This is not an actual error - rather just an indication
 * that something requested the replication should stop.
 */
export class ReplicationAbortedError extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S1103;

  constructor(description?: string) {
    super({
      code: ReplicationAbortedError.CODE,
      description: description ?? 'Replication aborted'
    });
  }
}

export class UnsupportedMediaType extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S2004;

  constructor(errors: any) {
    super({
      code: UnsupportedMediaType.CODE,
      status: 415,
      description: 'Unsupported Media Type',
      details: errors
    });
  }
}

export class AuthorizationError extends ServiceError {
  /**
   * String describing the token. Does not contain the full token, but may help with debugging.
   * Safe for logs.
   */
  tokenDetails: string | undefined;
  /**
   * String describing related configuration. Should never be returned to the client.
   * Safe for logs.
   */
  configurationDetails: string | undefined;

  constructor(
    code: ErrorCode,
    description: string,
    options?: { tokenDetails?: string; configurationDetails?: string; cause?: any }
  ) {
    super({
      code,
      status: 401,
      description
    });
    this.cause = options?.cause;
    this.tokenDetails = options?.tokenDetails;
    this.configurationDetails = options?.configurationDetails;
  }
}

export class InternalServerError extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S2001;

  constructor(err: Error) {
    super({
      code: InternalServerError.CODE,
      severity: ErrorSeverity.ERROR,
      status: 500,
      description: 'Something went wrong',
      details: err.message,
      stack: process.env.NODE_ENV !== 'production' ? err.stack : undefined
    });
  }
}

export class RouteNotFound extends ServiceError {
  static readonly CODE = ErrorCode.PSYNC_S2002;

  constructor(path: string) {
    super({
      code: RouteNotFound.CODE,
      status: 404,
      description: 'The path does not exist on this server',
      details: `The path ${JSON.stringify(path)} does not exist on this server`,
      severity: ErrorSeverity.INFO
    });
  }
}

export class DatabaseConnectionError extends ServiceError {
  public cause: any;

  constructor(code: ErrorCode, message: string, cause: any) {
    super({
      code: code,
      status: 500,
      description: message,
      details: `cause: ${cause.message}`,
      stack: process.env.NODE_ENV !== 'production' ? cause.stack : undefined
    });
    this.cause = cause;
  }
}
