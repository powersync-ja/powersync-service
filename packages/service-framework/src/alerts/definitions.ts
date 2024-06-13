import * as errors from '../errors/errors-index';

export type Primitive = string | number | boolean;

export type CaptureOptions = {
  level?: errors.ErrorSeverity;
  tags?: Record<string, Primitive | undefined>;
  metadata?: Record<string, Primitive | undefined>;
};

export type CaptureErrorFunction = (error: any, options?: CaptureOptions) => void;
export type CaptureMessageFunction = (message: string, options?: CaptureOptions) => void;

export type ErrorReporter = {
  captureException: CaptureErrorFunction;
  captureMessage: CaptureMessageFunction;
};
