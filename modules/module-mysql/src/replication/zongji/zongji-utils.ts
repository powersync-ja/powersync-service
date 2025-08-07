import {
  BinLogEvent,
  BinLogGTIDLogEvent,
  BinLogRowEvent,
  BinLogRotationEvent,
  BinLogTableMapEvent,
  BinLogRowUpdateEvent,
  BinLogXidEvent,
  BinLogQueryEvent,
  BinLogHeartbeatEvent,
  BinLogHeartbeatEvent_V2
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

export function eventIsHeartbeat(event: BinLogEvent): event is BinLogHeartbeatEvent {
  return event.getEventName() == 'heartbeat';
}

export function eventIsHeartbeat_v2(event: BinLogEvent): event is BinLogHeartbeatEvent_V2 {
  return event.getEventName() == 'heartbeat_v2';
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

export function eventIsQuery(event: BinLogEvent): event is BinLogQueryEvent {
  return event.getEventName() == 'query';
}
