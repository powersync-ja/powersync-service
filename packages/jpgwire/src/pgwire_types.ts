// Adapted from https://github.com/kagis/pgwire/blob/0dc927f9f8990a903f238737326e53ba1c8d094f/mod.js#L2218

import { JsonContainer } from '@powersync/service-jsonbig';
import { dateToSqlite, lsnMakeComparable, timestampToSqlite, timestamptzToSqlite } from './util.js';

export class PgType {
  static decode(text: string, typeOid: number) {
    switch (
      typeOid // add line here when register new type
    ) {
      case 25 /* text    */:
      case 2950 /* uuid    */:
      case 1043 /* varchar */:
        return text;
      case 16 /* bool    */:
        return text == 't';
      case 17 /* bytea   */:
        return this._decodeBytea(text);
      case 21 /* int2    */:
      case 23 /* int4    */:
      case 26 /* oid     */:
      case 20 /* int8    */:
        return BigInt(text);
      case 700 /* float4  */:
      case 701 /* float8  */:
        return Number(text);
      case 1082 /* date */:
        return dateToSqlite(text);
      case 1114 /* timestamp */:
        return timestampToSqlite(text);
      case 1184 /* timestamptz */:
        return timestamptzToSqlite(text);
      case 114 /* json    */:
      case 3802 /* jsonb   */:
        // Don't parse the contents
        return new JsonContainer(text);
      case 3220 /* pg_lsn  */:
        return lsnMakeComparable(text);
    }
    const elemTypeid = this._elemTypeOid(typeOid);
    if (elemTypeid) {
      return this._decodeArray(text, elemTypeid);
    }
    return text; // unknown type
  }
  static _elemTypeOid(arrayTypeOid: number) {
    // select 'case ' || typarray || ': return ' || oid || '; // ' || typname from pg_catalog.pg_type WHERE typearray != 0;
    switch (
      arrayTypeOid // add line here when register new type
    ) {
      case 1000:
        return 16; // bool
      case 1001:
        return 17; // bytea
      case 1002:
        return 18; // char
      case 1003:
        return 19; // name
      case 1016:
        return 20; // int8
      case 1005:
        return 21; // int2
      case 1006:
        return 22; // int2vector
      case 1007:
        return 23; // int4
      case 1008:
        return 24; // regproc
      case 1009:
        return 25; // text
      case 1028:
        return 26; // oid
      case 1010:
        return 27; // tid
      case 1011:
        return 28; // xid
      case 1012:
        return 29; // cid
      case 1013:
        return 30; // oidvector
      case 210:
        return 71; // pg_type
      case 270:
        return 75; // pg_attribute
      case 272:
        return 81; // pg_proc
      case 273:
        return 83; // pg_class
      case 199:
        return 114; // json
      case 143:
        return 142; // xml
      case 271:
        return 5069; // xid8
      case 1017:
        return 600; // point
      case 1018:
        return 601; // lseg
      case 1019:
        return 602; // path
      case 1020:
        return 603; // box
      case 1027:
        return 604; // polygon
      case 629:
        return 628; // line
      case 1021:
        return 700; // float4
      case 1022:
        return 701; // float8
      case 0:
        return 705; // unknown
      case 719:
        return 718; // circle
      case 791:
        return 790; // money
      case 1040:
        return 829; // macaddr
      case 1041:
        return 869; // inet
      case 651:
        return 650; // cidr
      case 775:
        return 774; // macaddr8
      case 1034:
        return 1033; // aclitem
      case 1014:
        return 1042; // bpchar
      case 1015:
        return 1043; // varchar
      case 1182:
        return 1082; // date
      case 1183:
        return 1083; // time
      case 1115:
        return 1114; // timestamp
      case 1185:
        return 1184; // timestamptz
      case 1187:
        return 1186; // interval
      case 1270:
        return 1266; // timetz
      case 1561:
        return 1560; // bit
      case 1563:
        return 1562; // varbit
      case 1231:
        return 1700; // numeric
      case 2201:
        return 1790; // refcursor
      case 2207:
        return 2202; // regprocedure
      case 2208:
        return 2203; // regoper
      case 2209:
        return 2204; // regoperator
      case 2210:
        return 2205; // regclass
      case 4192:
        return 4191; // regcollation
      case 2211:
        return 2206; // regtype
      case 4097:
        return 4096; // regrole
      case 4090:
        return 4089; // regnamespace
      case 2951:
        return 2950; // uuid
      case 3221:
        return 3220; // pg_lsn
      case 3643:
        return 3614; // tsvector
      case 3644:
        return 3642; // gtsvector
      case 3645:
        return 3615; // tsquery
      case 3735:
        return 3734; // regconfig
      case 3770:
        return 3769; // regdictionary
      case 3807:
        return 3802; // jsonb
      case 4073:
        return 4072; // jsonpath
    }
  }
  static _decodeArray(text: string, elemTypeOid: number): any {
    text = text.replace(/^\[.+=/, ''); // skip dimensions
    let result: any;
    for (let i = 0, inQuotes = false, elStart = 0, stack: any[] = []; i < text.length; i++) {
      const ch = text.charCodeAt(i);
      if (ch == 0x5c /*\*/) {
        i++; // escape
      } else if (ch == 0x22 /*"*/) {
        inQuotes = !inQuotes;
      } else if (inQuotes) {
      } else if (ch == 0x7b /*{*/) {
        // continue
        stack.unshift([]), (elStart = i + 1);
      } else if (ch == 0x7d /*}*/ || ch == 0x2c /*,*/) {
        // TODO configurable delimiter
        // TODO ensure .slice is cheap enough to do it unconditionally
        const escaped = text.slice(elStart, i); // TODO trim ' \t\n\r\v\f'
        if (result) {
          stack[0].push(result);
        } else if (/^NULL$/i.test(escaped)) {
          stack[0].push(null);
        } else if (escaped.length) {
          const unescaped = escaped.replace(/^"|"$|(?<!\\)\\/g, '');
          // TODO accept decodeFn as argument,
          // extract parseArray logic out of decoder,
          // do benchmark of static vs dynamic dispatch
          const decoded = this.decode(unescaped, elemTypeOid);
          stack[0].push(decoded);
        }
        result = ch == 0x7d /*}*/ && stack.shift();
        elStart = i + 1; // TODO dry
      }
    }
    return result;
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
