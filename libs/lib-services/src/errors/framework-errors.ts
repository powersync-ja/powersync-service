export enum ErrorSeverity {
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error'
}

type Digit = '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9';
type Category = 'S' | 'R';

// Note: This generates a type union of 20k possiblities,
// which could potentially slow down the TypeScript compiler.
// If it does, we could switch to a simpler `PSYNC_${Category}${number}` type.
export type ServiceErrorCode = `PSYNC_${Category}${Digit}${Digit}${Digit}${Digit}`;

export type ErrorData = {
  name?: string;

  code: ServiceErrorCode;
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

  private static errorMessage(data: ErrorData) {
    let message = `[${data.code}] ${data.description}`;
    if (data.details) {
      message += `\n  ${data.details}`;
    }
    return message;
  }

  constructor(data: ErrorData) {
    super(ServiceError.errorMessage(data));

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
  static readonly CODE = 'PSYNC_S2001';
  constructor(errors: any) {
    super({
      code: ValidationError.CODE,
      status: 400,
      description: 'Validation failed',
      details: JSON.stringify(errors)
    });
  }
}

export class AuthorizationError extends ServiceError {
  static readonly CODE = 'PSYNC_S2101';
  constructor(errors: any) {
    super({
      code: AuthorizationError.CODE,
      status: 401,
      description: 'Authorization failed',
      details: errors
    });
  }
}

export class InternalServerError extends ServiceError {
  static readonly CODE = 'PSYNC_S2001';
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
  static readonly CODE = 'PSYNC_S2002';

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
