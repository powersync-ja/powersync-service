import {
  BinLogEvent,
  BinLogGTIDLogEvent,
  BinLogRowEvent,
  BinLogRotationEvent,
  BinLogTableMapEvent,
  BinLogRowUpdateEvent,
  BinLogXidEvent
} from '@powersync/mysql-zongji';

export function eventIsGTIDLog(event: BinLogEvent): event is BinLogGTIDLogEvent {
  return event.getEventName() == 'gtidlog';
}

export function eventIsTableMap(event: BinLogEvent): event is BinLogTableMapEvent {
  return event.getEventName() == 'tablemap';
}

export function eventIsXid(event: BinLogEvent): event is BinLogXidEvent {
  return event.getEventName() == 'xid';
}

export function eventIsRotation(event: BinLogEvent): event is BinLogRotationEvent {
  return event.getEventName() == 'rotate';
}

export function eventIsWriteMutation(event: BinLogEvent): event is BinLogRowEvent {
  return event.getEventName() == 'writerows';
}

export function eventIsDeleteMutation(event: BinLogEvent): event is BinLogRowEvent {
  return event.getEventName() == 'deleterows';
}

export function eventIsUpdateMutation(event: BinLogEvent): event is BinLogRowUpdateEvent {
  return event.getEventName() == 'updaterows';
}
