import ajvModule from 'ajv';
// Hack to make this work both in NodeJS and a browser
const Ajv = ajvModule.default ?? ajvModule;
const ajv = new Ajv({ allErrors: true, verbose: true });

export const syncRulesSchema: ajvModule.Schema = {
  type: 'object',
  properties: {
    bucket_definitions: {
      type: 'object',
      description: 'List of bucket definitions',
      examples: [{ global: { data: 'select * from mytable' } }],
      patternProperties: {
        '.*': {
          type: 'object',
          required: ['data'],
          examples: [{ data: ['select * from mytable'] }],
          properties: {
            parameters: {
              description: 'Parameter query(ies)',
              anyOf: [
                { type: 'string', description: 'Parameter query' },
                {
                  type: 'array',
                  description: 'Parameter queries',
                  items: {
                    type: 'string'
                  }
                }
              ]
            },
            data: {
              type: 'array',
              description: 'Data queries',
              items: {
                type: 'string'
              }
            }
          },
          additionalProperties: false
        }
      }
    }
  },
  required: ['bucket_definitions'],
  additionalProperties: false
} as const;

export const validateSyncRulesSchema: any = ajv.compile(syncRulesSchema);
