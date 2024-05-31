export type PromiseFunction<T> = (context: MutexContext) => Promise<T>;
export type SharedPromiseFunction<T> = (context: SharedMutexContext) => Promise<T>;

const DEBUG_MUTEX = false;

interface Task {
  exclusive: boolean;
  execute: (context: SharedMutexContext) => void;
}

export interface SharedMutexContext {
  sharedLock<T>(promiseFn: SharedPromiseFunction<T>): Promise<T>;
}

export interface MutexContext extends SharedMutexContext {
  exclusiveLock<T>(promiseFn: PromiseFunction<T>): Promise<T>;
}

class ExclusiveContext implements MutexContext {
  constructor(private mutex: Mutex) {}

  exclusiveLock<T>(promiseFn: PromiseFunction<T>): Promise<T> {
    return promiseFn(this);
  }

  sharedLock<T>(promiseFn: PromiseFunction<T>): Promise<T> {
    return promiseFn(this);
  }
}

class SharedContext implements SharedMutexContext {
  constructor(private mutex: Mutex) {}

  async exclusiveLock<T>(promiseFn: PromiseFunction<T>): Promise<T> {
    throw new Error('Cannot upgrade a shared lock to an exclusive lock.');
  }

  sharedLock<T>(promiseFn: SharedPromiseFunction<T>): Promise<T> {
    return promiseFn(this);
  }
}

/**
 * Mutex maintains a queue of Promise-returning functions that
 * are executed sequentially (whereas normally they would execute their async code concurrently).
 */
export class Mutex implements MutexContext {
  private queue: Task[];
  private sharedCount: number;
  private exclusiveLocked: boolean;

  constructor() {
    this.queue = [];
    this.sharedCount = 0;
    this.exclusiveLocked = false;
  }

  /**
   * Place a function on the queue.
   * The function may either return a Promise or a value.
   * Return a Promise that is resolved with the result of the function.
   */
  async exclusiveLock<T>(promiseFn: PromiseFunction<T>): Promise<T> {
    return this.lock(promiseFn as any, true);
  }

  /**
   * Place a function on the queue.
   * This function may execute in parallel with other "multi" functions, but not with other functions on the exclusive
   * queue.
   */
  sharedLock<T>(promiseFn: SharedPromiseFunction<T>): Promise<T> {
    return this.lock(promiseFn, false);
  }

  private async lock<T>(promiseFn: SharedPromiseFunction<T>, exclusive?: boolean): Promise<T> {
    const context = await this._lockNext(exclusive);
    let timeout;
    try {
      if (DEBUG_MUTEX) {
        const stack = new Error().stack;
        timeout = setTimeout(() => {
          console.warn('Mutex not released in 10 seconds\n', stack);
        }, 10000);
      }
      return await promiseFn(context);
    } finally {
      if (DEBUG_MUTEX) {
        clearTimeout(timeout);
      }
      if (!exclusive) {
        this.sharedCount -= 1;
      } else {
        this.exclusiveLocked = false;
      }
      this._tryNext();
    }
  }

  /**
   * Convert a normal Promise-returning function into one that is automatically enqueued.
   * The signature of the function stays the same - only the execution is potentially delayed.
   * The only exception is that if the function would have returned a scalar value, it now
   * returns a Promise.
   */
  qu<T>(fn: PromiseFunction<T>): () => Promise<T> {
    var self = this;
    return function () {
      var args = arguments;
      return self.exclusiveLock(function () {
        return fn.apply(null, args as any);
      });
    };
  }

  /**
   * Wait until we are ready to execute the next task on the queue.
   *
   * This places a "Task" marker on the queue, and waits until we get to it.
   *
   * @param exclusive
   */
  private _lockNext(exclusive?: boolean): Promise<SharedMutexContext> {
    var self = this;
    return new Promise<SharedMutexContext>(function (resolve, reject) {
      const task: Task = {
        execute: resolve,
        exclusive: exclusive ?? false
      };
      self.queue.push(task);
      self._tryNext();
    });
  }

  private _tryNext() {
    if (this.queue.length == 0) {
      return false;
    }

    if (this.exclusiveLocked) {
      return false;
    }

    var task = this.queue[0];
    if (!task.exclusive) {
      this.sharedCount += 1;
      this.queue.shift();
      task.execute(new SharedContext(this));
    } else if (this.sharedCount == 0) {
      this.exclusiveLocked = true;
      this.queue.shift();
      task.execute(new ExclusiveContext(this));
    } else {
      return false;
    }

    return true;
  }
}
