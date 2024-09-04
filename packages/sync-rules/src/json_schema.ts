import ajvModule from 'ajv';
// Hack to make this work both in NodeJS and a browser
const Ajv = ajvModule.default ?? ajvModule;
const ajv = new Ajv({ allErrors: true, verbose: true, allowUnionTypes: true });

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
            accept_potentially_dangerous_queries: {
              description: 'If true, disables warnings on potentially dangerous queries',
              type: 'boolean'
            },
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
    },
    event_definitions: {
      type: 'object',
      description: 'Record of sync replication event definitions',
      examples: [
        { write_checkpoint: 'select user_id, client_id, checkpoint from write_checkpoints' },
        {
          write_checkpoint: ['select user_id, client_id, checkpoint from write_checkpoints']
        }
      ],
      patternProperties: {
        '.*': {
          type: ['string', 'array'],
          items: {
            type: 'string'
          },
          minItems: 1,
          uniqueItems: true
        }
      }
    }
  },
  required: ['bucket_definitions'],
  additionalProperties: false
} as const;

export const validateSyncRulesSchema: any = ajv.compile(syncRulesSchema);
