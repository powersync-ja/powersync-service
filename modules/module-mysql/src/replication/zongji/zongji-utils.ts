import {
  BinLogEvent,
  BinLogGTIDLogEvent,
  BinLogMutationEvent,
  BinLogRotationEvent,
  BinLogUpdateEvent,
  BinLogXidEvent
} from '@powersync/mysql-zongji';

export function eventIsGTIDLog(event: BinLogEvent): event is BinLogGTIDLogEvent {
  return event.getEventName() == 'gtidlog';
}

export function eventIsXid(event: BinLogEvent): event is BinLogXidEvent {
  return event.getEventName() == 'xid';
}

export function eventIsRotation(event: BinLogEvent): event is BinLogRotationEvent {
  return event.getEventName() == 'rotate';
}

export function eventIsWriteMutation(event: BinLogEvent): event is BinLogMutationEvent {
  return event.getEventName() == 'writerows';
}

export function eventIsDeleteMutation(event: BinLogEvent): event is BinLogMutationEvent {
  return event.getEventName() == 'deleterows';
}

export function eventIsUpdateMutation(event: BinLogEvent): event is BinLogUpdateEvent {
  return event.getEventName() == 'updaterows';
}
