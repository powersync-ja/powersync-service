import { WriteCheckpointBatcher } from './write-checkpoint-batcher.js';

export interface CreateWriteCheckpointOptions {
  userId: string | undefined;
  clientId: string | undefined;
  checkpointRequestId?: bigint;
  batcher: WriteCheckpointBatcher;
}
export async function createWriteCheckpoint(options: CreateWriteCheckpointOptions) {
  const full_user_id = checkpointUserId(options.userId, options.clientId);

  return options.batcher.enqueue(full_user_id, options.checkpointRequestId);
}

export function checkpointUserId(user_id: string | undefined, client_id: string | undefined) {
  if (user_id == null) {
    throw new Error('user_id is required');
  }
  if (client_id == null) {
    return user_id;
  }
  return `${user_id}/${client_id}`;
}
