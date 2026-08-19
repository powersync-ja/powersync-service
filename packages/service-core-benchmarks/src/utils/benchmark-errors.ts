import { BenchmarkError, BenchmarkErrorPhase } from '../types/BenchmarkError.js';

export function toBenchmarkError(phase: BenchmarkErrorPhase, error: unknown): BenchmarkError {
  if (isError(error)) {
    return {
      phase,
      type: readStringProperty(error, 'name') || readConstructorName(error) || 'Error',
      message: readStringProperty(error, 'message') ?? stringifyThrownValue(error),
      stack: readStringProperty(error, 'stack')
    };
  }

  return {
    phase,
    type: 'NonError',
    message: stringifyThrownValue(error),
    stack: null
  };
}

function isError(value: unknown): value is Error {
  try {
    return value instanceof Error;
  } catch {
    return false;
  }
}

function readStringProperty(value: object, property: string): string | null {
  try {
    const propertyValue = Reflect.get(value, property);
    return typeof propertyValue === 'string' ? propertyValue : null;
  } catch {
    return null;
  }
}

function readConstructorName(value: object): string | null {
  try {
    const constructor = Reflect.get(value, 'constructor');
    return typeof constructor === 'function' ? readStringProperty(constructor, 'name') : null;
  } catch {
    return null;
  }
}

function stringifyThrownValue(value: unknown): string {
  if (typeof value === 'string') {
    return value;
  }

  try {
    const serialized = JSON.stringify(value);
    if (serialized != null) {
      return serialized;
    }
  } catch {
    // Fall through to String for values unsupported by JSON.stringify.
  }

  try {
    return String(value);
  } catch {
    return 'Unknown thrown value';
  }
}
