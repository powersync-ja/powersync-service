import ajvModule from 'ajv';
import { CompatibilityEdition, CompatibilityOption, TimeValuePrecision } from './compatibility.js';
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
            accept_potentially_dangerous_queries: {
              description: 'If true, disables warnings on potentially dangerous queries',
              type: 'boolean'
            },
            priority: {
              description: 'Priority for the bucket (lower values indicate higher priority).',
              type: 'integer'
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
    streams: {
      type: 'object',
      description: 'List of stream definitions',
      examples: [{ user_details: { query: 'select * from users where id = auth.user_id()' } }],
      patternProperties: {
        '.*': {
          type: 'object',
          examples: [{ query: ['select * from mytable'] }],
          properties: {
            accept_potentially_dangerous_queries: {
              description: 'If true, disables warnings on potentially dangerous queries',
              type: 'boolean'
            },
            auto_subscribe: {
              description: 'Whether clients will subscribe to this stream by default.',
              type: 'boolean'
            },
            priority: {
              description: 'Priority for the bucket (lower values indicate higher priority).',
              type: 'integer'
            },
            query: {
              description: 'The SQL query defining content to sync in this stream.',
              type: 'string'
            },
            queries: {
              description: 'SQL queries defining content to sync in this stream.',
              type: 'array',
              items: { type: 'string' }
            },
            with: {
              type: 'object',
              description: 'Common table expressions defining subqueries available in queries.',
              patternProperties: {
                '.*': {
                  type: 'string'
                }
              }
            }
          }
        }
      }
    },
    event_definitions: {
      type: 'object',
      description: 'Record of sync replication event definitions',
      examples: [
        {
          write_checkpoints: {
            payloads: ['select user_id, client_id, checkpoint from checkpoints']
          }
        }
      ],
      patternProperties: {
        '.*': {
          type: ['object'],
          required: ['payloads'],
          examples: [{ payloads: ['select user_id, client_id, checkpoint from checkpoints'] }],
          properties: {
            payloads: {
              description: 'Queries which extract event payload fields from replicated table rows.',
              type: 'array',
              items: {
                type: 'string'
              }
            },
            additionalProperties: false,
            uniqueItems: true
          }
        }
      }
    },
    config: {
      type: 'object',
      description: 'Config declaring the compatibility level used to parse these definitions.',
      properties: {
        edition: {
          type: 'integer',
          default: CompatibilityEdition.LEGACY,
          minimum: CompatibilityEdition.LEGACY,
          exclusiveMaximum: CompatibilityEdition.COMPILED_STREAMS + 1
        },
        timestamp_max_precision: {
          type: 'string',
          enum: Object.values(TimeValuePrecision.byName).map((e) => e.name)
        },
        ...Object.fromEntries(
          Object.entries(CompatibilityOption.byName).map((e) => {
            return [
              e[0],
              {
                type: 'boolean',
                description: `Enabled by default starting from edition ${e[1].fixedIn}: ${e[1].description}`
              }
            ];
          })
        )
      },
      additionalProperties: false
    }
  },
  anyOf: [{ required: ['bucket_definitions'] }, { required: ['streams'] }],
  additionalProperties: false
} as const;

export const validateSyncRulesSchema: any = ajv.compile(syncRulesSchema);
