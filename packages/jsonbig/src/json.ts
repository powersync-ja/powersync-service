import { isInteger, Replacer, JavaScriptValue } from 'lossless-json';
import { JsonContainer } from './json_container.js';
import { stringify } from './json_stringify.js';

const numberStringifier = {
  test: (value: any) => typeof value == 'number',
  stringify: (value: unknown) => {
    const text = (value as number).toString();
    if (isInteger(text)) {
      // Used to preserve the "type".
      return text + '.0';
    } else {
      return text;
    }
  }
};

// Avoid a parse + serialize round-trip for JSON data.
const jsonContainerStringifier = {
  test: (value: any) => value instanceof JsonContainer,
  stringify: (value: unknown) => {
    return (value as JsonContainer).toString();
  }
};

const stringifiers = [numberStringifier, jsonContainerStringifier];

export const JSONBig = {
  parse(text: string): JavaScriptValue {
    return JSON.parse(text, (_key: string, value: any, context?: any) => {
      if (typeof value == 'number') {
        // The context arg requires Node v22+
        const rawValue: string = context!.source;
        return isInteger(rawValue) ? BigInt(rawValue) : value;
      }
      return value;
    });
  },
  stringify(value: any, replacer?: Replacer, space?: string | number): string {
    return stringify(value, replacer, space, stringifiers)!;
  }
};
