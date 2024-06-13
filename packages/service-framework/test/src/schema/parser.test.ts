import { describe, test, expect } from 'vitest';

import * as framework_schema from '../../../src/schema/schema-index';

describe('schema-tools', () => {
  test('it should correctly prune unused definitions', () => {
    const schema = framework_schema.parseJSONSchema({
      definitions: {
        // unused, should be stripped out
        a: {
          type: 'object',
          properties: {
            prop: {
              type: 'string'
            }
          },
          required: ['prop']
        },

        // extended reference, should be included after walking the full schema
        b: {
          type: 'object',
          properties: {
            prop: {
              type: 'string'
            }
          },
          required: ['prop']
        },
        b1: {
          type: 'object',
          properties: {
            prop: {
              $ref: '#/definitions/b'
            }
          },
          required: ['prop']
        },

        // circular reference, should not result in the walker getting stuck
        c: {
          type: 'object',
          properties: {
            prop: {
              $ref: '#/definitions/c'
            }
          },
          required: ['prop']
        }
      },

      type: 'object',
      properties: {
        a: {
          type: 'object',
          properties: {
            a: {
              $ref: '#/definitions/b1'
            },
            b: {
              enum: ['A']
            }
          }
        },
        b: {
          oneOf: [
            {
              type: 'object',
              properties: {
                a: {
                  $ref: '#/definitions/c'
                }
              },
              required: ['a'],
              additionalProperties: false
            }
          ]
        }
      }
    });

    expect(schema.compile()).toMatchSnapshot();
  });
});
