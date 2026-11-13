/**
 * Pauses production of new snapshot pages while streaming is backlogged.
 * Shared by replication jobs in this process; never wait here while holding a
 * storage writer lock, or gate metadata, accepted writes, or streaming repairs.
 */
export class SnapshotGate {
  private readonly backlogs = new Set<object>();
  private readonly waiters = new Set<() => void>();

  get isPaused(): boolean {
    return this.backlogs.size > 0;
  }

  /** One registration per streaming attempt; disposal releases failed/stopped jobs. */
  trackStreaming() {
    const token = {};
    const gate = this;
    let disposed = false;
    const setBacklogged = (value: boolean) => {
      if (disposed) {
        return;
      }
      if (value) {
        this.backlogs.add(token);
      } else if (this.backlogs.delete(token) && !this.isPaused) {
        const waiters = [...this.waiters];
        this.waiters.clear();
        for (const resume of waiters) {
          resume();
        }
      }
    };
    return {
      get isBacklogged() {
        return gate.backlogs.has(token);
      },
      setBacklogged,
      [Symbol.dispose]: () => {
        setBacklogged(false);
        disposed = true;
      }
    };
  }

  async wait(signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    while (this.isPaused) {
      await new Promise<void>((resolve, reject) => {
        const resume = () => {
          signal.removeEventListener('abort', abort);
          this.waiters.delete(resume);
          resolve();
        };
        const abort = () => {
          signal.removeEventListener('abort', abort);
          this.waiters.delete(resume);
          reject(signal.reason);
        };
        this.waiters.add(resume);
        signal.addEventListener('abort', abort, { once: true });
        if (signal.aborted) {
          abort();
        }
      });
      // Another job can become backlogged before this continuation runs.
      signal.throwIfAborted();
    }
  }
}

export const snapshotGate = new SnapshotGate();
