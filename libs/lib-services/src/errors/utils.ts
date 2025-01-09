import { ErrorData, InternalServerError, ServiceError } from './framework-errors.js';

export const isServiceError = (err: any): err is ServiceError => {
  const matches = ServiceError.isServiceError(err);
  return !!matches;
};

export const asServiceError = (err: any): ServiceError => {
  if (ServiceError.isServiceError(err)) {
    return err;
  } else {
    return new InternalServerError(err);
  }
};

export const getErrorData = (err: Error | any): ErrorData | undefined => {
  if (!isServiceError(err)) {
    return;
  }
  return err.toJSON();
};

export const matchesErrorCode = (err: Error | any, code: string) => {
  if (isServiceError(err)) {
    return err.errorData.code === code;
  }
  return false;
};
