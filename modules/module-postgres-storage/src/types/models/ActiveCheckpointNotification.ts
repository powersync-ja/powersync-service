import * as t from 'ts-codec';
import { jsonb } from '../codecs.js';
import { ActiveCheckpoint } from './ActiveCheckpoint.js';

export const ActiveCheckpointPayload = t.object({
  active_checkpoint: ActiveCheckpoint
});

export type ActiveCheckpointPayload = t.Encoded<typeof ActiveCheckpointPayload>;
export type ActiveCheckpointPayloadDecoded = t.Decoded<typeof ActiveCheckpointPayload>;

export const ActiveCheckpointNotification = jsonb(ActiveCheckpointPayload);
export type ActiveCheckpointNotification = t.Encoded<typeof ActiveCheckpointNotification>;
export type ActiveCheckpointNotificationDecoded = t.Decoded<typeof ActiveCheckpointNotification>;
