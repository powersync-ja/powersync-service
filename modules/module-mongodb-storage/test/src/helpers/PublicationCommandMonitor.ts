import { mongo } from '@powersync/lib-service-mongodb';

interface CommandSample {
  name: string;
  startedAt: number;
  endedAt?: number;
  failed?: boolean;
  transaction?: TransactionSample;
}

interface TransactionSample {
  key: string;
  startedAt: number;
  endedAt?: number;
  outcome?: 'committed' | 'aborted';
  replication: boolean;
  commands: CommandSample[];
}

/**
 * Counts actual wire commands, including getMore, aborted attempts and commit retries.
 * Attach after setup/warmup to keep index creation and allocator refills separate.
 * Store timing and classification only, never source data or command payloads.
 */
export class PublicationCommandMonitor implements Disposable {
  readonly commands: CommandSample[] = [];
  readonly transactions = new Map<string, TransactionSample>();
  private readonly requests = new Map<number, CommandSample>();

  constructor(private readonly client: mongo.MongoClient) {
    client.on('commandStarted', this.started);
    client.on('commandSucceeded', this.succeeded);
    client.on('commandFailed', this.failed);
  }

  get replicationAttempts() {
    return [...this.transactions.values()].filter((transaction) => transaction.replication);
  }

  get activeReplicationAttempts() {
    return this.replicationAttempts.filter((transaction) => transaction.endedAt == null);
  }

  summary() {
    const counts = (commands: CommandSample[]) => {
      const result: Record<string, number> = {};
      for (const command of commands) {
        result[command.name] = (result[command.name] ?? 0) + 1;
      }
      return result;
    };
    return {
      commands: counts(this.commands),
      outsideTransaction: counts(this.commands.filter((command) => command.transaction == null)),
      replicationAttempts: this.replicationAttempts.map((transaction) => ({
        outcome: transaction.outcome,
        activeMs: transaction.endedAt == null ? null : transaction.endedAt - transaction.startedAt,
        commands: counts(transaction.commands),
        failedCommands: transaction.commands.filter((command) => command.failed).length
      })),
      otherTransactions: [...this.transactions.values()].filter((transaction) => !transaction.replication).length
    };
  }

  private started = (event: mongo.CommandStartedEvent) => {
    const command = event.command;
    const startedAt = performance.now();
    let transaction: TransactionSample | undefined;
    // txnNumber alone also appears on retryable writes outside transactions.
    if (command.autocommit === false) {
      const key = `${command.lsid.id.toString('hex')}:${command.txnNumber.toString()}`;
      transaction = this.transactions.get(key);
      if (transaction == null) {
        transaction = { key, startedAt, replication: false, commands: [] };
        this.transactions.set(key, transaction);
      }
      // The writer's first command is the conditional stream fence. Compaction
      // shares the client but does not fence the replication stream document.
      transaction.replication ||= event.commandName === 'findAndModify' && command.findAndModify === 'sync_rules';
    }
    const sample: CommandSample = { name: event.commandName, startedAt, transaction };
    this.commands.push(sample);
    transaction?.commands.push(sample);
    this.requests.set(event.requestId, sample);
  };

  private finish(requestId: number, failed: boolean, errorCode?: number) {
    const sample = this.requests.get(requestId);
    if (sample == null) {
      return;
    }
    this.requests.delete(requestId);
    sample.endedAt = performance.now();
    sample.failed = failed;
    // A failed commit can have an unknown outcome and may be retried. Only a
    // successful commit or a confirmed abort closes the observed attempt.
    // NoSuchTransaction also confirms that the server already aborted it.
    if (
      sample.transaction != null &&
      ((sample.name === 'abortTransaction' && (!failed || errorCode === 251)) ||
        (sample.name === 'commitTransaction' && !failed))
    ) {
      sample.transaction.endedAt = sample.endedAt;
      sample.transaction.outcome = sample.name === 'commitTransaction' ? 'committed' : 'aborted';
    }
  }

  private succeeded = (event: mongo.CommandSucceededEvent) => this.finish(event.requestId, false);
  private failed = (event: mongo.CommandFailedEvent) => {
    const errorCode = event.failure instanceof mongo.MongoServerError ? event.failure.code : undefined;
    this.finish(event.requestId, true, typeof errorCode === 'number' ? errorCode : undefined);
  };

  [Symbol.dispose]() {
    this.client.off('commandStarted', this.started);
    this.client.off('commandSucceeded', this.succeeded);
    this.client.off('commandFailed', this.failed);
  }
}
