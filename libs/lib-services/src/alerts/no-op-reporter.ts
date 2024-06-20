import { ErrorReporter } from './definitions.js';

export const NoOpReporter: ErrorReporter = {
  captureException: () => {},
  captureMessage: () => {}
};
