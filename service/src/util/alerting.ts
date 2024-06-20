import * as sentry_types from '@sentry/types';
import * as sentry from '@sentry/node';
import { utils } from '@powersync/service-core';
import * as framework from '@powersync/service-framework';

// Generally ignore errors that are due to configuration issues, rather than
// service bugs.
// These can cause massive volumes of errors on Sentry, and don't add value.
const IGNORE_TYPES = ['AbortError', 'AuthorizationError'];

const IGNORE_MESSAGES: RegExp[] = [
  /**
   * Self hosted cases might want to be notified about these
   * messages.
   */
  // /^getaddrinfo ENOTFOUND/,
  // /^connect ECONNREFUSED/,
  // /^Timeout while connecting/,
  // /^certificate has expired/
];

export const createSentryReporter = (opts?: {
  beforeSend?: (event: sentry_types.Event, hint: sentry_types.EventHint) => any;
}): framework.ErrorReporter => {
  if (process.env.SENTRY_DSN) {
    sentry.init({
      dsn: process.env.SENTRY_DSN,
      release: process.env.SHA,
      environment: process.env.MICRO_ENVIRONMENT_NAME,
      serverName: process.env.HOST_NAME,
      beforeSend: opts?.beforeSend ? opts.beforeSend : undefined
    });
  } else {
    framework.logger.debug(
      'Alerts configured with sentry reporter but no SENTRY_DSN environment variable has been set'
    );
  }

  return {
    captureException: (error, options) => {
      sentry.captureException(error, options);
    },
    captureMessage: (message, options) => {
      sentry.captureMessage(message, options);
    }
  };
};

export const sentryErrorReporter = createSentryReporter({
  beforeSend: (event, _hint) => {
    const error = event.exception?.values?.[0];
    if (error?.type != null && IGNORE_TYPES.includes(error.type)) {
      return;
    }
    const message = error?.value ?? '';
    for (let re of IGNORE_MESSAGES) {
      if (re.test(message)) {
        return;
      }
    }

    // Inject our tags
    event.tags = Object.assign({}, utils.getGlobalTags(), event.tags);
    return event;
  }
});
