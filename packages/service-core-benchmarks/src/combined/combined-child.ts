import { container } from '@powersync/lib-services-framework';
import {
  COMBINED_CHILD_PROTOCOL_VERSION,
  CombinedChildCommand,
  CombinedChildEvent,
  serializeCombinedChildError
} from './combined-child-protocol.js';
import { CombinedChildRuntime } from './combined-child-runtime.js';

container.registerDefaults();

const runtime = new CombinedChildRuntime();
const seenRequestIds = new Set<string>();
let activeRunId: string | undefined;
let activeIterationId: string | undefined;
let commandQueue = Promise.resolve();

process.on('message', (message: unknown) => {
  commandQueue = commandQueue.then(() => handle(message));
});

async function handle(message: unknown): Promise<void> {
  const command = message as CombinedChildCommand;
  try {
    assertCommand(command);
    const payload = await runtime.execute(command);
    if (command.kind === 'setup_iteration') activeIterationId = command.iterationId;
    if (command.kind === 'cleanup_iteration') activeIterationId = undefined;
    send({
      protocolVersion: COMBINED_CHILD_PROTOCOL_VERSION,
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
      protocolVersion: COMBINED_CHILD_PROTOCOL_VERSION,
      direction: 'event',
      kind: 'fatal',
      runId: typeof command?.runId === 'string' ? command.runId : (activeRunId ?? 'unknown'),
      iterationId: command?.iterationId,
      requestId: command?.requestId,
      error: serializeCombinedChildError(error)
    });
  }
}

function assertCommand(command: CombinedChildCommand): void {
  if (
    command == null ||
    command.protocolVersion !== COMBINED_CHILD_PROTOCOL_VERSION ||
    command.direction !== 'command' ||
    typeof command.runId !== 'string' ||
    typeof command.requestId !== 'string'
  ) {
    throw new Error('Invalid combined child command envelope');
  }
  if (activeRunId == null) activeRunId = command.runId;
  if (command.runId !== activeRunId) throw new Error(`Received combined child command for stale run ${command.runId}`);
  if (seenRequestIds.has(command.requestId)) {
    throw new Error(`Received duplicate combined child request ${command.requestId}`);
  }
  seenRequestIds.add(command.requestId);

  const iterationCommand = ['setup_iteration', 'release_replication', 'collect_evidence', 'cleanup_iteration'].includes(
    command.kind
  );
  if (iterationCommand !== (typeof command.iterationId === 'string' && command.iterationId.length > 0)) {
    throw new Error(`Invalid iteration id for combined child command ${command.kind}`);
  }
  if (command.kind === 'setup_iteration' && activeIterationId != null) {
    throw new Error(`Combined child iteration ${activeIterationId} is already active`);
  }
  if (
    ['release_replication', 'collect_evidence', 'cleanup_iteration'].includes(command.kind) &&
    command.iterationId !== activeIterationId
  ) {
    throw new Error(`Received combined child command for stale iteration ${command.iterationId}`);
  }
}

function send(event: CombinedChildEvent): void {
  if (process.send == null) throw new Error('Combined child requires an IPC channel');
  process.send(event);
}
