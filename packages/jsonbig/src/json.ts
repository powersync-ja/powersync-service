import * as json from 'lossless-json';
import { isInteger, NumberParser, Replacer, Reviver, JavaScriptValue } from 'lossless-json';
import { JsonContainer } from './json_container.js';
import { stringify } from './json_stringify.js';

const numberParser: NumberParser = (value) => {
  return isInteger(value) ? BigInt(value) : parseFloat(value);
};

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
  parse(text: string, reviver?: Reviver): JavaScriptValue {
    return json.parse(text, reviver, numberParser);
  },
  stringify(value: any, replacer?: Replacer, space?: string | number): string {
    return stringify(value, replacer, space, stringifiers)!;
  }
};
