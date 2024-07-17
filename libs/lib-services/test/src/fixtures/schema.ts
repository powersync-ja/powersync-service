export default {
  definitions: {
    c: {
      type: 'object',
      properties: {
        prop: {
          type: 'string'
        }
      },
      required: ['prop']
    }
  },

  type: 'object',
  properties: {
    name: {
      type: 'object',
      properties: {
        a: {
          type: 'string'
        },
        b: {
          enum: ['A']
        }
      },
      required: ['a'],
      additionalProperties: false
    },
    b: {
      oneOf: [
        {
          type: 'object',
          properties: {
            a: {
              type: 'number'
            }
          },
          required: ['a'],
          additionalProperties: false
        }
      ]
    },
    d: {
      $ref: '#/definitions/c'
    }
  },
  required: ['name', 'b'],
  additionalProperties: false
};
