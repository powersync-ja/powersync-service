import { ErrorData, JourneyError } from './framework-errors.js';

export const isJourneyError = (err: any): err is JourneyError => {
  const matches = err instanceof JourneyError || err.is_journey_error;
  return !!matches;
};

export const getErrorData = (err: Error | any): ErrorData | undefined => {
  if (!isJourneyError(err)) {
    return;
  }
  return err.toJSON();
};

export const matchesErrorCode = (err: Error | any, code: string) => {
  if (isJourneyError(err)) {
    return err.errorData.code === code;
  }
  return false;
};
