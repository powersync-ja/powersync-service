import { JSONBig } from './json.js';

/**
 * Store a JSON text value. Used to mark the value as JSON-sourced, but only parse if really needed.
 *
 * Unlike JSON.rawJSON, this can be used for arrays and objects.
 */
export class JsonContainer {
  readonly data: string;

  constructor(data: string) {
    this.data = data;
  }

  parsed() {
    return JSONBig.parse(this.data);
  }

  /**
   *
   * @returns The JSON representation
   */
  toString() {
    return this.data;
  }

  /**
   *
   * @returns The parsed representation
   */
  toJSON() {
    return this.parsed();
  }
}
