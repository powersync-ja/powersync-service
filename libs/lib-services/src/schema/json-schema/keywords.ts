import * as ajv from 'ajv';

export const BufferNodeType: ajv.KeywordDefinition = {
  keyword: 'nodeType',
  metaSchema: {
    type: 'string',
    enum: ['bigint', 'buffer', 'date']
  },
  error: {
    message: ({ schemaCode }) => {
      return ajv.str`should be a ${schemaCode}`;
    }
  },
  code(context) {
    switch (context.schema) {
      case 'bigint': {
        return context.fail(ajv._`typeof ${context.data} != 'bigint'`);
      }
      case 'buffer': {
        return context.fail(ajv._`!Buffer.isBuffer(${context.data})`);
      }
      case 'date': {
        return context.fail(ajv._`!(${context.data} instanceof Date)`);
      }
      default: {
        context.fail(ajv._`true`);
      }
    }
  }
};
