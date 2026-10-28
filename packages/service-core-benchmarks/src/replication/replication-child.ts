import { container } from '@powersync/lib-services-framework';
import {
  REPLICATION_CHILD_PROTOCOL_VERSION,
  ReplicationChildCommand,
  ReplicationChildEvent,
  serializeError
} from './replication-child-protocol.js';
import { ReplicationChildRuntime } from './replication-child-runtime.js';

container.registerDefaults();

const runtime = new ReplicationChildRuntime();

process.on('message', (message: unknown) => {
  void handle(message as ReplicationChildCommand);
});

async function handle(command: ReplicationChildCommand): Promise<void> {
  try {
    assertCommand(command);
    const payload = await runtime.execute(command);
    send({
      protocolVersion: REPLICATION_CHILD_PROTOCOL_VERSION,
      direction: 'event',
      kind: 'response',
      runId: command.runId,
      iterationId: command.iterationId,
      requestId: command.requestId,
      command: command.kind,
      payload
    });
    if (command.kind === 'shutdown' || command.kind === 'abort') {
      process.disconnect();
      setImmediate(() => process.exit(command.kind === 'shutdown' ? 0 : 1));
    }
  } catch (error) {
    send({
      protocolVersion: REPLICATION_CHILD_PROTOCOL_VERSION,
      direction: 'event',
      kind: 'fatal',
      runId: command.runId,
      iterationId: command.iterationId,
      requestId: command.requestId,
      error: serializeError(error)
    });
  }
}

function assertCommand(command: ReplicationChildCommand): void {
  if (command.protocolVersion !== REPLICATION_CHILD_PROTOCOL_VERSION || command.direction !== 'command') {
    throw new Error('Invalid replication child command envelope');
  }
}

function send(event: ReplicationChildEvent): void {
  if (process.send == null) throw new Error('Replication child requires an IPC channel');
  process.send(event);
}
