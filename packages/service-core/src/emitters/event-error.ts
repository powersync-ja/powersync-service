import { EventNames } from './emitter-interfaces.js';

export class EventError extends Error {
  constructor(
    public eventName: EventNames,
    message: string
  ) {
    super(message);
  }
}
