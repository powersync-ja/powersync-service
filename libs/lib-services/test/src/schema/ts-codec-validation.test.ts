import * as t from 'ts-codec';
import { describe, test, expect } from 'vitest';

import * as framework_schema from '../../../src/schema/schema-index.js';

describe('ts-codec validation', () => {
  enum Values {
    A = 'A',
    B = 'B'
  }

  const codec = t.object({
    name: t.string,
    surname: t.string,
    other: t.object({
      a: t.array(t.string),
      b: t.literal('optional').optional()
    }),
    tuple: t.tuple([t.string, t.number]),
    or: t.number.or(t.string),
    enum: t.Enum(Values),

    complex: t
      .object({
        a: t.string
      })
      .and(
        t.object({
          b: t.number
        })
      )
      .and(
        t.object({
          c: t
            .object({
              a: t.string
            })
            .and(
              t
                .object({
                  b: t.boolean
                })
                .or(
                  t.object({
                    c: t.number
                  })
                )
            )
        })
      )
  });

  test('passes validation for codec', () => {
    const validator = framework_schema.createTsCodecValidator(codec);

    const result = validator.validate({
      name: 'a',
      surname: 'b',
      other: {
        a: ['nice'],
        b: 'optional'
      },
      tuple: ['string', 1],
      or: 1,
      enum: Values.A,

      complex: {
        a: '',
        b: 1,
        c: {
          a: '',
          b: true
        }
      }
    });

    expect(result.valid).toBe(true);
  });

  test('fails validation for runtime codec', () => {
    const validator = framework_schema.createTsCodecValidator(codec);

    const result = validator.validate({
      // @ts-ignore
      name: 1,
      other: {
        a: ['nice'],
        // @ts-ignore
        b: 'op'
      },
      // @ts-ignore
      tuple: [1, 1],
      // @ts-ignore
      enum: 'c',
      // @ts-ignore
      or: [],
      // @ts-ignore
      complex: {}
    });

    expect(result).toMatchSnapshot();
  });
});
