// Adapted from https://github.com/kagis/pgwire/blob/0dc927f9f8990a903f238737326e53ba1c8d094f/mod.js#L2218

import { JsonContainer } from '@powersync/service-jsonbig';
import { CustomSqliteValue, TimeValue, type DatabaseInputValue } from '@powersync/service-sync-rules';
import { dateToSqlite, lsnMakeComparable, timestampToSqlite, timestamptzToSqlite } from './util.js';
import {
  arrayDelimiters,
  CHAR_CODE_COMMA,
  CHAR_CODE_LEFT_BRACE,
  CHAR_CODE_RIGHT_BRACE,
  decodeArray,
  decodeSequence,
  Delimiters,
  SequenceListener
} from './sequence_tokenizer.js';

export enum PgTypeOid {
  TEXT = 25,
  UUID = 2950,
  VARCHAR = 1043,
  BOOL = 16,
  BYTEA = 17,
  INT2 = 21,
  INT4 = 23,
  OID = 26,
  INT8 = 20,
  FLOAT4 = 700,
  FLOAT8 = 701,
  DATE = 1082,
  TIMESTAMP = 1114,
  TIMESTAMPTZ = 1184,
  TIME = 1083,
  JSON = 114,
  JSONB = 3802,
  PG_LSN = 3220
}

// Generate using:
//   select '[' || typarray || ', ' || oid || '], // ' || typname from pg_catalog.pg_type WHERE typarray != 0;
export const ARRAY_TO_ELEM_OID = new Map<number, number>([
  [1000, 16], // bool
  [1001, 17], // bytea
  [1002, 18], // char
  [1003, 19], // name
  [1016, 20], // int8
  [1005, 21], // int2
  [1006, 22], // int2vector
  [1007, 23], // int4
  [1008, 24], // regproc
  [1009, 25], // text
  [1028, 26], // oid
  [1010, 27], // tid
  [1011, 28], // xid
  [1012, 29], // cid
  [1013, 30], // oidvector
  [210, 71], // pg_type
  [270, 75], // pg_attribute
  [272, 81], // pg_proc
  [273, 83], // pg_class
  [199, 114], // json
  [143, 142], // xml
  [271, 5069], // xid8
  [1017, 600], // point
  [1018, 601], // lseg
  [1019, 602], // path
  [1020, 603], // box
  [1027, 604], // polygon
  [629, 628], // line
  [1021, 700], // float4
  [1022, 701], // float8
  [0, 705], // unknown
  [719, 718], // circle
  [791, 790], // money
  [1040, 829], // macaddr
  [1041, 869], // inet
  [651, 650], // cidr
  [775, 774], // macaddr8
  [1034, 1033], // aclitem
  [1014, 1042], // bpchar
  [1015, 1043], // varchar
  [1182, 1082], // date
  [1183, 1083], // time
  [1115, 1114], // timestamp
  [1185, 1184], // timestamptz
  [1187, 1186], // interval
  [1270, 1266], // timetz
  [1561, 1560], // bit
  [1563, 1562], // varbit
  [1231, 1700], // numeric
  [2201, 1790], // refcursor
  [2207, 2202], // regprocedure
  [2208, 2203], // regoper
  [2209, 2204], // regoperator
  [2210, 2205], // regclass
  [4192, 4191], // regcollation
  [2211, 2206], // regtype
  [4097, 4096], // regrole
  [4090, 4089], // regnamespace
  [2951, 2950], // uuid
  [3221, 3220], // pg_lsn
  [3643, 3614], // tsvector
  [3644, 3642], // gtsvector
  [3645, 3615], // tsquery
  [3735, 3734], // regconfig
  [3770, 3769], // regdictionary
  [3807, 3802], // jsonb
  [4073, 4072] // jsonpath
]);

const ELEM_OID_TO_ARRAY = new Map<number, number>();
ARRAY_TO_ELEM_OID.forEach((value, key) => {
  ELEM_OID_TO_ARRAY.set(value, key);
});

export class PgType {
  static getArrayType(typeOid: number): number | undefined {
    return ELEM_OID_TO_ARRAY.get(typeOid);
  }

  static decode(text: string, typeOid: number): DatabaseInputValue {
    switch (typeOid) {
      // add line here when register new type
      case PgTypeOid.TEXT:
      case PgTypeOid.UUID:
      case PgTypeOid.VARCHAR:
        return text;
      case PgTypeOid.BOOL:
        return text == 't';
      case PgTypeOid.BYTEA:
        return this._decodeBytea(text);
      case PgTypeOid.INT2:
      case PgTypeOid.INT4:
      case PgTypeOid.OID:
      case PgTypeOid.INT8:
        return BigInt(text);
      case PgTypeOid.FLOAT4:
      case PgTypeOid.FLOAT8:
        return Number(text);
      case PgTypeOid.DATE:
        return dateToSqlite(text);
      case PgTypeOid.TIMESTAMP:
        return timestampToSqlite(text);
      case PgTypeOid.TIMESTAMPTZ:
        return timestamptzToSqlite(text);
      case PgTypeOid.TIME:
        return TimeValue.parse(text);
      case PgTypeOid.JSON:
      case PgTypeOid.JSONB:
        // Don't parse the contents
        return new JsonContainer(text);
      case PgTypeOid.PG_LSN:
        return lsnMakeComparable(text);
    }
    const elemTypeid = this.elemTypeOid(typeOid);
    if (elemTypeid != null) {
      return this._decodeArray(text, elemTypeid);
    }
    return text; // unknown type
  }

  static elemTypeOid(arrayTypeOid: number): number | undefined {
    // select 'case ' || typarray || ': return ' || oid || '; // ' || typname from pg_catalog.pg_type WHERE typarray != 0;
    return ARRAY_TO_ELEM_OID.get(arrayTypeOid);
  }

  static _decodeArray(text: string, elemTypeOid: number): DatabaseInputValue[] {
    text = text.replace(/^\[.+=/, ''); // skip dimensions
    return decodeArray({
      source: text,
      decodeElement: (raw) => PgType.decode(raw, elemTypeOid)
    });
  }

  static _decodeBytea(text: string): Uint8Array {
    // https://www.postgresql.org/docs/9.6/datatype-binary.html#AEN5830
    if (text.startsWith('\\x')) {
      const hex = text.slice(2); // TODO check hex.length is even ?
      const bytes = Uint8Array.from(Buffer.from(hex, 'hex'));
      return bytes;
    }
    // Adapted from original
    throw new Error('Not supported');
  }
  static _encodeBytea(bytes: Uint8Array): string {
    return '\\x' + Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  }
}
