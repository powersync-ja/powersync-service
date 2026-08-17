import {
  ReplicationBenchmarkManifest,
  ReplicationBenchmarkTarget,
  ReplicationBenchmarkTransaction
} from '../../../types/ReplicationBenchmark.js';

export type SyntheticReplicationEvent =
  | { readonly kind: 'transaction'; readonly transaction: ReplicationBenchmarkTransaction }
  | { readonly kind: 'keepalive'; readonly position: string };

interface VisiblePosition {
  readonly position: string;
  readonly visibleAtNs: string;
}

interface PositionWaiter {
  readonly resolve: (position: VisiblePosition) => void;
  readonly reject: (error: Error) => void;
}

export class SyntheticReplicationSource {
  private readonly released = deferred<string>();
  private readonly snapshot = deferred<VisiblePosition>();
  private readonly events: SyntheticReplicationEvent[] = [];
  private readonly eventWaiters: Array<(event: SyntheticReplicationEvent | null) => void> = [];
  private readonly visiblePositions = new Map<string, VisiblePosition>();
  private readonly positionWaiters = new Map<string, PositionWaiter[]>();
  private readonly committedTransactions = new Set<string>();
  private stopped = false;
  private keepaliveIndex = 0;
  private failure?: Error;

  constructor(readonly manifest: ReplicationBenchmarkManifest) {}

  releaseReplication(): string {
    const releasedAtNs = process.hrtime.bigint().toString();
    this.released.resolve(releasedAtNs);
    return releasedAtNs;
  }

  waitForRelease(): Promise<string> {
    return this.released.promise;
  }

  markSnapshotVisible(position: string): void {
    const observation = { position, visibleAtNs: process.hrtime.bigint().toString() };
    this.visiblePositions.set(position, observation);
    this.visiblePositions.set(this.manifest.snapshotRows.at(-1)!.id, observation);
    this.snapshot.resolve(observation);
    this.resolvePositionWaiters(observation, this.manifest.snapshotRows.at(-1)!.id);
  }

  waitForSnapshot(): Promise<VisiblePosition> {
    return this.snapshot.promise;
  }

  commitTransaction(transactionId: string): ReplicationBenchmarkTarget {
    const transaction = this.manifest.transactions.find((candidate) => candidate.id === transactionId);
    if (transaction == null) throw new Error(`Unknown synthetic transaction ${transactionId}`);
    if (this.committedTransactions.has(transactionId)) {
      throw new Error(`Synthetic transaction ${transactionId} has already been committed`);
    }
    this.committedTransactions.add(transactionId);
    this.enqueue({ kind: 'transaction', transaction });
    return {
      markerId: transaction.mutations.at(-1)!.row.id,
      nativePosition: transaction.position,
      committedAtNs: process.hrtime.bigint().toString()
    };
  }

  keepalive(): ReplicationBenchmarkTarget {
    const lastTransactionPosition = this.manifest.transactions.at(-1)?.position ?? snapshotPosition();
    const position = (BigInt(lastTransactionPosition) + BigInt(++this.keepaliveIndex)).toString().padStart(20, '0');
    this.enqueue({ kind: 'keepalive', position });
    return {
      markerId: `keepalive-${this.keepaliveIndex}`,
      nativePosition: position,
      committedAtNs: process.hrtime.bigint().toString()
    };
  }

  async nextEvent(signal: AbortSignal): Promise<SyntheticReplicationEvent | null> {
    if (this.events.length > 0) return this.events.shift()!;
    if (this.stopped || signal.aborted) return null;
    return await new Promise<SyntheticReplicationEvent | null>((resolve) => {
      const onAbort = () => {
        const index = this.eventWaiters.indexOf(waiter);
        if (index >= 0) this.eventWaiters.splice(index, 1);
        resolve(null);
      };
      const waiter = (event: SyntheticReplicationEvent | null) => {
        signal.removeEventListener('abort', onAbort);
        resolve(event);
      };
      this.eventWaiters.push(waiter);
      signal.addEventListener('abort', onAbort, { once: true });
    });
  }

  markTargetVisible(markerId: string, position: string): void {
    const observation = { position, visibleAtNs: process.hrtime.bigint().toString() };
    this.visiblePositions.set(markerId, observation);
    this.visiblePositions.set(position, observation);
    this.resolvePositionWaiters(observation, markerId);
  }

  waitForTarget(markerId: string): Promise<VisiblePosition> {
    return this.waitForKey(markerId);
  }

  waitForPosition(position: string): Promise<VisiblePosition> {
    return this.waitForKey(position);
  }

  stop(): void {
    this.stopped = true;
    for (const waiter of this.eventWaiters.splice(0)) waiter(null);
  }

  fail(error: unknown): void {
    if (this.failure != null) return;
    this.failure = error instanceof Error ? error : new Error(String(error));
    this.snapshot.reject(this.failure);
    for (const waiters of this.positionWaiters.values()) {
      for (const waiter of waiters) waiter.reject(this.failure);
    }
    this.positionWaiters.clear();
    this.stop();
  }

  private enqueue(event: SyntheticReplicationEvent): void {
    if (this.stopped) throw new Error('Synthetic replication source is stopped');
    const waiter = this.eventWaiters.shift();
    if (waiter == null) this.events.push(event);
    else waiter(event);
  }

  private waitForKey(key: string): Promise<VisiblePosition> {
    if (this.failure != null) return Promise.reject(this.failure);
    const existing = this.visiblePositions.get(key);
    if (existing != null) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const waiters = this.positionWaiters.get(key) ?? [];
      waiters.push({ resolve, reject });
      this.positionWaiters.set(key, waiters);
    });
  }

  private resolvePositionWaiters(observation: VisiblePosition, additionalKey?: string): void {
    for (const key of [observation.position, additionalKey].filter((value): value is string => value != null)) {
      for (const waiter of this.positionWaiters.get(key) ?? []) waiter.resolve(observation);
      this.positionWaiters.delete(key);
    }
  }
}

export function snapshotPosition(): string {
  return '00000000000000000001';
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolver, rejecter) => {
    resolve = resolver;
    reject = rejecter;
  });
  return { promise, resolve, reject };
}
