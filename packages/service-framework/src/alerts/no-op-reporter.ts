import { ErrorReporter } from './definitions';

export const NoOpReporter: ErrorReporter = {
  captureException: () => {},
  captureMessage: () => {}
};
