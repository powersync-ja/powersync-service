import * as micro from '@journeyapps-platform/micro';
import { utils } from '@powersync/service-core';

// Generally ignore errors that are due to configuration issues, rather than
// service bugs.
// These can cause massive volumes of errors on Sentry, and don't add value.
// These errors are better debugged via Collide.

const IGNORE_TYPES = [
  'PgError.28P01', // password authentication failed
  'AbortError',
  'AuthorizationError'
];

const IGNORE_MESSAGES = [
  /^getaddrinfo ENOTFOUND/,
  /^connect ECONNREFUSED/,
  /^Timeout while connecting/,
  /^certificate has expired/
];

micro.alerts.register({
  reporter: micro.alerts.reporters.createSentryReporter({
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
  })
});
