import * as t from 'ts-codec';
import * as bson from 'bson';
import { DateTimeValue } from '@powersync/service-sync-rules';

export const buffer = t.codec<Buffer, string>(
  'Buffer',
  (buffer) => {
    if (!Buffer.isBuffer(buffer)) {
      throw new t.TransformError([`Expected buffer but got ${typeof buffer}`]);
    }
    return buffer.toString('base64');
  },
  (buffer) => Buffer.from(buffer, 'base64')
);

export const date = t.codec<Date, string | DateTimeValue>(
  'Date',
  (date) => {
    if (!(date instanceof Date)) {
      throw new t.TransformError([`Expected Date but got ${typeof date}`]);
    }
    return date.toISOString();
  },
  (date) => {
    // In our jpgwire wrapper, we patch the row decoding logic to map timestamps into TimeValue instances, so we need to
    // support those here.
    const parsed = new Date(date instanceof DateTimeValue ? date.iso8601Representation : date);
    if (isNaN(parsed.getTime())) {
      throw new t.TransformError([`Invalid date`]);
    }
    return parsed;
  }
);

const assertObjectId = (value: any) => {
  if (!bson.ObjectId.isValid(value)) {
    throw new t.TransformError([`Expected an ObjectId but got ${typeof value}`]);
  }
};
export const ObjectId = t.codec<bson.ObjectId, string>(
  'ObjectId',
  (id) => {
    assertObjectId(id);
    return id.toHexString();
  },
  (id) => {
    assertObjectId(id);
    return new bson.ObjectId(id);
  }
);

const assertObjectWithField = (field: string, data: any) => {
  if (typeof data !== 'object') {
    throw new t.TransformError([`Expected an object but got ${typeof data}`]);
  }
  if (!(field in data)) {
    throw new t.TransformError([`Expected ${field} to be a member of object`]);
  }
};
export const ResourceId = t.codec<{ _id: bson.ObjectId }, { id: string }>(
  'ResourceId',
  (data) => {
    assertObjectWithField('_id', data);
    return {
      id: ObjectId.encode(data._id)
    };
  },
  (data) => {
    assertObjectWithField('id', data);
    return {
      _id: ObjectId.decode(data.id)
    };
  }
);

export const Timestamps = t.object({
  created_at: date,
  updated_at: date
});

export const Resource = ResourceId.and(Timestamps);

export const QueryFilter = t.object({
  exists: t.boolean
});
