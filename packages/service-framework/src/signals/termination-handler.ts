import _ from 'lodash';
import { logger } from '../logger/Logger';

export enum Signal {
  SIGTERM = 'SIGTERM',
  SIGINT = 'SIGINT',
  SIGUSR2 = 'SIGUSR2'
}

export type Handler = (event: Signal) => void | Promise<void>;

type TerminationHandlerParams = {
  /**
   * A list of process signals to listen for.
   *
   * @default ['SIGTERM', 'SIGINT', 'SIGUSR2']
   */
  signals?: Signal[];

  /**
   * The timeout for all handlers to complete. Once this is reached the process will be exited
   */
  timeout_ms?: number;
};

/**
 * Utility function to handle external termination signals. Calls an async handler
 * and then kills the application.
 */
export const createTerminationHandler = (params?: TerminationHandlerParams) => {
  const { signals = Object.values(Signal), timeout_ms = 30000 } = params || {};

  const handlers: Handler[] = [];

  let signal_received = false;
  const signalHandler = (signal: Signal) => {
    if (signal === Signal.SIGINT) {
      logger.info('Send ^C again to force exit');
    }

    if (signal_received) {
      // The SIGINT signal is sent on ctrl-c - if the user presses ctrl-c twice then we
      // hard exit
      if (signal === Signal.SIGINT) {
        logger.info('Received second ^C. Exiting');
        process.exit(1);
      }
      return;
    }

    signal_received = true;

    new Promise<void>(async (resolve) => {
      logger.info('Terminating gracefully ...');

      for (const handler of handlers) {
        try {
          await handler(signal);
        } catch (err) {
          logger.error('Failed to execute termination handler', err);
        }
      }

      logger.info('Exiting');
      resolve();
    }).then(() => {
      process.exit(0);
    });

    setTimeout(() => {
      logger.error('Timed out waiting for program to exit. Force exiting');
      process.exit(1);
    }, timeout_ms);
  };

  // This debounce is needed as certain executors (like npm, pnpm) seem to send kill signals multiple times. This debounce
  // prevents the termination handler from immediately exiting under those circumstances
  const debouncedSignalHandler = _.debounce(signalHandler, 1000, {
    leading: true,
    trailing: false
  });

  for (const signal of signals) {
    process.on(signal, () => debouncedSignalHandler(signal));
  }

  return {
    /**
     * Register a termination handler to be run when a termination signal is received. Calling
     * this function will register the termination handler at the start of the list resulting
     * in handlers being called in the reverse order to how they were registered.
     *
     * Use `handleTerminationSignalLast` if you want to register the handler at the end.
     */
    handleTerminationSignal: (handler: Handler) => {
      handlers.unshift(handler);
    },

    /**
     * This is the same as `handleTerminationSignal` except it will register a termination handler
     * at the end of the list - resulting in it being run after existing termination handlers.
     *
     * Use `handleTerminationSignal` if you want to register the handler at the start.
     */
    handleTerminationSignalLast: (handler: Handler) => {
      handlers.push(handler);
    },

    gracefully: async <T>(
      exec: () => T | Promise<T>,
      handler: (component: T, signal: Signal) => Promise<void> | void
    ) => {
      const component = await exec();
      handlers.unshift((signal) => {
        return handler(component, signal);
      });
      return component;
    }
  };
};

export type TerminationHandler = ReturnType<typeof createTerminationHandler>;
