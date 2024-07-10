export enum ErrorSeverity {
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error'
}

export type ErrorData = {
  name?: string;

  code: string;
  description: string;

  severity?: ErrorSeverity;
  details?: string;
  status?: number;
  stack?: string;

  origin?: string;

  trace_id?: string;
};

// Maybe this could be renamed to ServiceError or something similar
export class JourneyError extends Error {
  is_journey_error = true;

  errorData: ErrorData;

  static isJourneyError(input: any): input is JourneyError {
    return input instanceof JourneyError || input?.is_journey_error == true;
  }

  private static errorMessage(data: ErrorData) {
    let message = `[${data.code}] ${data.description}`;
    if (data.details) {
      message += `\n  ${data.details}`;
    }
    return message;
  }

  constructor(data: ErrorData) {
    super(JourneyError.errorMessage(data));

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

export class ValidationError extends JourneyError {
  static CODE = 'VALIDATION_ERROR';
  constructor(errors: any) {
    super({
      code: ValidationError.CODE,
      status: 400,
      description: 'Validation failed',
      details: JSON.stringify(errors)
    });
  }
}

export class AuthorizationError extends JourneyError {
  static CODE = 'AUTHORIZATION';
  constructor(errors: any) {
    super({
      code: AuthorizationError.CODE,
      status: 401,
      description: 'Authorization failed',
      details: errors
    });
  }
}

export class InternalServerError extends JourneyError {
  static CODE = 'INTERNAL_SERVER_ERROR';
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

export class ResourceNotFound extends JourneyError {
  static CODE = 'RESOURCE_NOT_FOUND';

  /**
   * @deprecated Use the (resource, id) constructor instead.
   * @param id
   */
  constructor(id: string);
  constructor(resource: string, id: string);

  constructor(resource: string, id?: string) {
    const combinedId = id ? `${resource}/${id}` : resource;
    super({
      code: ResourceNotFound.CODE,
      status: 404,
      description: 'The requested resource does not exist on this server',
      details: `The resource ${combinedId} does not exist on this server`,
      severity: ErrorSeverity.INFO
    });
  }
}

export class ResourceConflict extends JourneyError {
  static CODE = 'RESOURCE_CONFLICT';

  constructor(details: string) {
    super({
      code: ResourceConflict.CODE,
      status: 409,
      description: 'The specified resource already exists on this server',
      details: details,
      severity: ErrorSeverity.INFO
    });
  }
}
