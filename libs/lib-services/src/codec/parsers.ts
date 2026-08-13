import * as t from 'ts-codec';
import * as codecs from './codecs.js';

export const ObjectIdParser = t.createParser<typeof codecs.ObjectId>(codecs.ObjectId._tag, (_, { target }) => {
  switch (target) {
    case t.TransformTarget.Encoded: {
      return { type: 'string' };
    }
    case t.TransformTarget.Decoded: {
      return { bsonType: 'ObjectId' };
    }
  }
});

export const ResourceIdParser = t.createParser<typeof codecs.ResourceId>(codecs.ResourceId._tag, (_, { target }) => {
  switch (target) {
    case t.TransformTarget.Encoded: {
      return {
        type: 'object',
        properties: {
          id: { type: 'string' }
        },
        required: ['id']
      };
    }
    case t.TransformTarget.Decoded: {
      return {
        type: 'object',
        properties: {
          _id: { bsonType: 'ObjectId' }
        },
        required: ['_id']
      };
    }
  }
});

export const DateParser = t.createParser<typeof codecs.date>(codecs.date._tag, (_, { target }) => {
  switch (target) {
    case t.TransformTarget.Encoded: {
      return { type: 'string' };
    }
    case t.TransformTarget.Decoded: {
      return { nodeType: 'date' };
    }
  }
});

export const BigIntParser = t.createParser<typeof codecs.bigint>(codecs.bigint._tag, (_, { target }) => {
  switch (target) {
    case t.TransformTarget.Encoded: {
      return {
        oneOf: [
          { type: 'string', pattern: '^-?[0-9]+$' },
          {
            type: 'integer',
            minimum: Number.MIN_SAFE_INTEGER,
            maximum: Number.MAX_SAFE_INTEGER
          }
        ]
      };
    }
    case t.TransformTarget.Decoded: {
      return { nodeType: 'bigint' };
    }
  }
});

export const BufferParser = t.createParser<typeof codecs.buffer>(codecs.buffer._tag, (_, { target }) => {
  switch (target) {
    case t.TransformTarget.Encoded: {
      return { type: 'string' };
    }
    case t.TransformTarget.Decoded: {
      return { nodeType: 'buffer' };
    }
  }
});

export const parsers = [ObjectIdParser, ResourceIdParser, DateParser, BigIntParser, BufferParser];
