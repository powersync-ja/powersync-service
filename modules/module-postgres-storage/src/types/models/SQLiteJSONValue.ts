import * as t from 'ts-codec';
import { bigint } from '../codecs.js';

export const SQLiteJSONValue = t.number.or(t.string).or(bigint).or(t.Null);
export type SQLiteJSONValue = t.Encoded<typeof SQLiteJSONValue>;
export type SQLiteJSONValueDecoded = t.Decoded<typeof SQLiteJSONValue>;

export const SQLiteJSONRecord = t.record(SQLiteJSONValue);
export type SQLiteJSONRecord = t.Encoded<typeof SQLiteJSONRecord>;
export type SQLiteJSONRecordDecoded = t.Decoded<typeof SQLiteJSONRecord>;
