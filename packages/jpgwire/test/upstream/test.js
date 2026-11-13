import { assertEquals } from './assert.js';

export function setup({ pgconnection, pgpool, test }) {
  test('simple proto', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var res = await conn.query(/*sql*/ `
        select null a;
        values ('hello', -42), ('привет', 42);
        do $$ begin raise notice 'sample message'; end $$;
      `);
    } finally {
      await conn.end();
    }

    // avoid ambiguous status for multistatement query
    assertEquals(res.status, null);

    // root result should be from last selectish statement
    assertEquals(res.scalar, 'hello'); // TODO deprecated
    assertEquals(res.rows, res.results[1].rows);
    assertEquals(res.columns, res.results[1].columns);
    assertEquals([...res], [...res.results[1].rows[0]]);

    assertEquals(res.results.length, 3);
    assertEquals(res.results[0].status, 'SELECT 1');
    assertEquals(res.results[0].scalar, null); // TODO deprecated
    assertEquals(res.results[0].columns, [
      { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: -1 }
    ]);
    // test null bypasses decoding
    assertEquals(res.results[0].rows.length, 1);
    assertEquals(res.results[0].rows[0][0], null);
    assertEquals(res.results[0].rows[0].raw, [null]);

    assertEquals(res.results[1].status, 'SELECT 2');
    assertEquals(res.results[1].scalar, 'hello'); // TODO deprecated
    assertEquals(res.results[1].columns, [
      { binary: 0, name: 'column1', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: -1 },
      { binary: 0, name: 'column2', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 23, typeSize: 4 }
    ]);
    const { rows } = res.results[1];
    assertEquals(rows.length, 2);
    {
      const row = rows[0];
      assertEquals(row.length, 2);
      assertEquals([row[0], row[1]], ['hello', -42]);
      assertEquals([...row], ['hello', -42]);
      assertEquals(row.raw, ['hello', '-42']);
    }
    {
      const row = rows[1];
      assertEquals(row.length, 2);
      assertEquals([row[0], row[1]], ['привет', 42]);
      assertEquals([...row], ['привет', 42]);
      assertEquals(row.raw, ['привет', '42']);
    }

    assertEquals(res.results[2].status, 'DO');
    assertEquals(res.results[2].scalar, undefined); // TODO deprecated
    assertEquals(res.results[2].columns, []);
    assertEquals(res.results[2].rows, []);

    assertEquals(res.notices.length, 1);
    assertEquals(res.notices[0].message, 'sample message');
    assertEquals(res.notices[0].code, '00000');
    assertEquals(res.notices[0].severity, 'NOTICE');
    assertEquals(res.notices[0].severityEn, 'NOTICE');
  });

  test('simple proto empty', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var res = await conn.query(/*sql*/ ``);
    } finally {
      await conn.end();
    }
    assertEquals([...res], []);
    assertEquals(res.status, 'EmptyQueryResponse');
    assertEquals(res.scalar, undefined); // TODO deprecated
    assertEquals(res.rows, []);
    assertEquals(res.columns, []);
    assertEquals(res.notices, []);
    assertEquals(res.results.length, 1);
    assertEquals(res.results[0].status, 'EmptyQueryResponse');
    assertEquals(res.results[0].scalar, undefined); // TODO deprecated
    assertEquals(res.results[0].rows, []);
    assertEquals(res.results[0].columns, []);
  });

  test('extended proto', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var res = await conn.query(
        {
          statement: /*sql*/ `do $$ begin raise notice 'sample message'; end $$`
        },
        {
          statement: String.raw/*sql*/ `select $1 a, $2 b`,
          binary: [false, true],
          params: [
            { type: 'int4', value: Uint8Array.of(1, 2, 3, 4) }, // binary param
            { type: 'int4', value: '-42' } // text param
          ]
        },
        {
          // PortalSuspended
          statement: /*sql*/ `values ('hello'), ('bonjour')`,
          limit: 1
        },
        {
          // EmptyQueryResponse
          statement: ''
        }
      );
    } finally {
      await conn.end();
    }

    // avoid ambiguous status for multistatement query
    assertEquals(res.status, null);

    // root result should be from last selectish statement
    assertEquals(res.scalar, 'hello'); // TODO deprecated
    assertEquals(res.rows, res.results[2].rows);
    assertEquals(res.columns, res.results[2].columns);
    assertEquals([...res], ['hello']);

    assertEquals(res.results.length, 4);

    assertEquals(res.results[0].status, 'DO');
    assertEquals(res.results[0].scalar, undefined); // TODO deprecated
    assertEquals(res.results[0].rows, []);
    assertEquals(res.results[0].columns, []);

    assertEquals(res.results[1].status, 'SELECT 1');
    assertEquals(res.results[1].scalar, 16909060); // TODO deprecated
    assertEquals(res.results[1].columns, [
      { binary: 0, name: 'a', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 23, typeSize: 4 },
      { binary: 1, name: 'b', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 23, typeSize: 4 }
    ]);
    assertEquals(res.results[1].rows.length, 1);
    const row = res.results[1].rows[0];
    assertEquals(row.length, 2);
    assertEquals(row[0], 16909060);
    assertEquals(row[1], Uint8Array.of(0xff, 0xff, 0xff, 0xd6)); // -42
    assertEquals([...row], [16909060, Uint8Array.of(0xff, 0xff, 0xff, 0xd6)]);
    assertEquals(row.raw, ['16909060', Uint8Array.of(0xff, 0xff, 0xff, 0xd6)]);

    assertEquals(res.results[2].status, 'PortalSuspended');
    assertEquals(res.results[2].scalar, 'hello'); // TODO deprecated
    assertEquals(res.results[2].columns, [
      { binary: 0, name: 'column1', tableColumn: 0, tableOid: 0, typeMod: -1, typeOid: 25, typeSize: -1 }
    ]);
    assertEquals(res.results[2].rows.length, 1);
    assertEquals(res.results[2].rows[0].length, 1);
    assertEquals(res.results[2].rows[0][0], 'hello');
    assertEquals([...res.results[2].rows[0]], ['hello']);
    assertEquals(res.results[2].rows[0].raw, ['hello']);

    assertEquals(res.results[3].status, 'EmptyQueryResponse');
    assertEquals(res.results[3].scalar, undefined); // TODO deprecated
    assertEquals(res.results[3].rows, []);
    assertEquals(res.results[3].columns, []);

    assertEquals(res.notices.length, 1);
    assertEquals(res.notices[0].message, 'sample message');
    assertEquals(res.notices[0].code, '00000');
    assertEquals(res.notices[0].severity, 'NOTICE');
    assertEquals(res.notices[0].severityEn, 'NOTICE');
  });

  // test('sync connection', async _ => {
  //   const { connection } = pgconnect('postgres://pgwire@pg:5432/postgres');
  //   try {
  //     await connection.query(/*sql*/ `select 'hello'`);
  //   } finally {
  //     await connection.end();
  //   }
  // });

  // test('connection error during query', async _ => {
  //   const { connection } = pgconnect('postgres://invalid@pg:5432/postgres');
  //   try {
  //     await assertRejects(
  //       _ => connection.query(/*sql*/ `select`),
  //       Error,
  //       '[PGERR_28000]'
  //     );
  //   } finally {
  //     await connection.end();
  //   }
  // });

  // test('throw when query after close', async _ => {
  //   const conn = await pgconnect('postgres://pgwire@pg:5432/postgres');
  //   await conn.end();
  //   await assertRejects(
  //     _ => conn.query(/*sql*/ `select`),
  //     Error,
  //     'postgres connection closed',
  //   );
  // });

  // test('pgliteral', async () => {
  //   assert.deepStrictEqual(pgwire.pgliteral(`foo`), `'foo'`);
  //   assert.deepStrictEqual(pgwire.pgliteral(`'foo'`), `'''foo'''`);
  //   assert.deepStrictEqual(pgwire.pgliteral(null), `NULL`);
  // });

  // test('pgident', async () => {
  //   assert.deepStrictEqual(pgwire.pgident(`foo`), `"foo"`);
  //   assert.deepStrictEqual(pgwire.pgident(`"foo"`), `"""foo"""`);
  //   assert.deepStrictEqual(pgwire.pgident(`public`, `foo`), `"public"."foo"`);
  // });

  test('connect with unexisting user, query', async (_) => {
    const conn = pgconnection('postgres://unicorn@pg:5432/postgres?_debug=0');
    try {
      const [res] = await Promise.allSettled([conn.query(/*sql*/ `select`)]);
      assertEquals(res.status, 'rejected');
      assertEquals(res.reason.name, 'PgError.28000');
      assertEquals(res.reason.cause.severity, 'FATAL');
      assertEquals(res.reason.cause.code, '28000');
    } finally {
      await conn.end();
    }
  });

  test('connect with unexisting user, pool', async (_) => {
    const pool = pgpool('postgres://unicorn@pg:5432/postgres?_debug=0');
    try {
      const [res] = await Promise.allSettled([pool.query(/*sql*/ `select`)]);
      assertEquals(res.status, 'rejected');
      assertEquals(res.reason.name, 'PgError.28000');
      assertEquals(res.reason.cause.severity, 'FATAL');
      assertEquals(res.reason.cause.code, '28000');
    } finally {
      await pool?.end();
    }
  });

  test('copy from stdin extended', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var [outDump] = await conn.query(
        { statement: /*sql*/ `create temp table greeting(id int, body text)` },
        { statement: /*sql*/ `copy greeting from stdin`, stdin: genInputDump() },
        { statement: /*sql*/ `select jsonb_agg(jsonb_build_array(id, body) order by id) from greeting` }
      );
    } finally {
      await conn.end();
    }
    async function* genInputDump() {
      const utf8enc = new TextEncoder();
      yield* ['-255\t', 'hello\n', '255\t', 'привет\n'].map(utf8enc.encode, utf8enc);
    }
    assertEquals(outDump, [
      [-255, 'hello'],
      [255, 'привет']
    ]);
  });

  test('copy from stdin extended missing', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var [res] = await Promise.allSettled([
        conn.query(
          { statement: /*sql*/ `create temp table greeting(id int, body text)` },
          { statement: /*sql*/ `copy greeting from stdin` }
        )
      ]);
    } finally {
      await conn.end();
    }
    assertEquals(res.status, 'rejected');
    assertEquals(String(res.reason), 'PgError.57014: COPY from stdin failed: no stdin provided');
  });

  test('copy from stdin simple', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var [outDump] = await conn.query(
        /*sql*/ `
        create temp table greeting(id int, body text);
        copy greeting from stdin;
        select jsonb_agg(jsonb_build_array(id, body) order by id) from greeting;
      `,
        { stdin: genInputDump() }
      );
    } finally {
      await conn.end();
    }
    async function* genInputDump() {
      const utf8enc = new TextEncoder();
      yield* ['-255\t', 'hello\n', '255\t', 'привет\n'].map(utf8enc.encode, utf8enc);
    }
    assertEquals(outDump, [
      [-255, 'hello'],
      [255, 'привет']
    ]);
  });

  test('copy from stdin simple 2', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      var [outDump] = await conn.query(
        /*sql*/ `
        create temp table greeting(id int, body text);
        copy greeting from stdin;
        copy greeting from stdin;
        select jsonb_agg(jsonb_build_array(id, body) order by id) from greeting;
      `,
        {
          stdins: [
            ['1\t', 'hello\n', '2\t', 'привет\n'].map(utf8encode),
            ['3\t', 'bonjour\n', '4\t', 'hola\n'].map(utf8encode)
          ]
        }
      );
    } finally {
      await conn.end();
    }
    function utf8encode(s) {
      return new TextEncoder().encode(s);
    }
    assertEquals(outDump, [
      [1, 'hello'],
      [2, 'привет'],
      [3, 'bonjour'],
      [4, 'hola']
    ]);
  });

  test('copy from stdin simple missing', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [res] = await Promise.allSettled([
        conn.query(
          /*sql*/ `
          create temp table test(foo int, bar text);
          copy test from stdin;
          select 'hello';
          copy test from stdin;
          copy test from stdin;
        `,
          {
            stdins: [
              ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode),
              ['1\t', 'hello\n', '2\t', 'world\n'].map(utf8encode)
              // here should be third one
            ]
          }
        )
      ]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'PgError.57014: COPY from stdin failed: no stdin provided');
    } finally {
      await conn.end();
    }
    function utf8encode(s) {
      return new TextEncoder().encode(s);
    }
  });

  test('copy to stdout', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const stdout = conn.stream(/*sql*/ `copy (values (1, 'hello'), (2, 'world')) to stdout`);
      const dump = await readAll(stdout);
      const dumps = new TextDecoder().decode(dump);
      assertEquals(dumps, '1\thello\n2\tworld\n');
    } finally {
      await conn.end();
    }
  });

  test('copy to stdout multi', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const stdout = conn.stream(/*sql*/ `
        copy (values (1, 'hello'), (2, 'world')) to stdout;
        copy (values (3, 'foo'), (4, 'bar')) to stdout;
      `);
      const dump = await readAll(stdout);
      const dumps = new TextDecoder().decode(dump);
      assertEquals(dumps, '1\thello\n2\tworld\n3\tfoo\n4\tbar\n');
    } finally {
      await conn.end();
    }
  });

  async function readAll(iter) {
    const chunks = [];
    let nbytes = 0;
    for await (const chunk of iter) {
      assertEquals(chunk instanceof Uint8Array, true);
      chunks.push(chunk);
      nbytes += chunk.byteLength;
    }
    const result = new Uint8Array(nbytes);
    let pos = 0;
    for (const chunk of chunks) {
      result.set(chunk, pos);
      pos += chunk.byteLength;
    }
    return result;
  }

  test('type decode', async (_) => {
    const statement = String.raw/*sql*/ `
      select null,
      true, false,
      'hello'::text,
      'hello'::varchar(100),
      '\xcafebabe'::bytea,
      42::int2, -42::int2,
      42::int4, -42::int4,
      42::int8, -42::int8,
      36.6::float4, -36.6::float4,
      36.6::float8, -36.6::float8,
      43::oid,
      jsonb_build_object('hello', 'world', 'num', 1),
      json_build_object('hello', 'world', 'num', 1),
      '1a/2b'::pg_lsn,
      'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid,
      array[1, 2, 3]::int[],
      array[1, 2, 3]::text[],
      array['"quoted"', '{string}', '"{-,-}"', e'\t'],
      array[[1, 2], [3, 4]],
      '[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}'::int[],
      array[1, null, 2],
      array[null]::text[],
      array[]::text[],
      array[]::int[]
    `;
    const expected = [
      null,
      true,
      false,
      'hello',
      'hello',
      Uint8Array.of(0xca, 0xfe, 0xba, 0xbe),
      42,
      -42,
      42,
      -42,
      42n,
      -42n,
      36.6,
      -36.6,
      36.6,
      -36.6,
      43,
      { hello: 'world', num: 1 },
      { hello: 'world', num: 1 },
      '0000001A/0000002B',
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
      [1, 2, 3],
      ['1', '2', '3'],
      ['"quoted"', '{string}', '"{-,-}"', '\t'],
      [
        [1, 2],
        [3, 4]
      ],
      [
        [
          [1, 2, 3],
          [4, 5, 6]
        ]
      ],
      [1, null, 2],
      [null],
      [],
      []
    ];
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [[...simple], [...ext]] = await Promise.all([conn.query(statement), conn.query({ statement })]);
      assertEquals(simple, expected);
      assertEquals(ext, expected);
    } finally {
      await conn.end();
    }
  });

  test('bytea decode escape formated', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [val] = await conn.query(String.raw/*sql*/ `
        set bytea_output = escape;
        select bytea 'hello\\'
          '\000\001\002\003\004\005\006\007\010\011\012\013\014\015\016\017'
          '\020\021\022\023\024\025\026\027\030\031\032\033\034\035\036\037'
          '\040\041\042\043\044\045\046\047\050\051\052\053\054\055\056\057'
          '\060\061\062\063\064\065\066\067\070\071\072\073\074\075\076\077'
          '\100\101\102\103\104\105\106\107\110\111\112\113\114\115\116\117'
          '\120\121\122\123\124\125\126\127\130\131\132\133\134\135\136\137'
          '\140\141\142\143\144\145\146\147\150\151\152\153\154\155\156\157'
          '\160\161\162\163\164\165\166\167\170\171\172\173\174\175\176\177'
          '\200\201\202\203\204\205\206\207\210\211\212\213\214\215\216\217'
          '\220\221\222\223\224\225\226\227\230\231\232\233\234\235\236\237'
          '\240\241\242\243\244\245\246\247\250\251\252\253\254\255\256\257'
          '\260\261\262\263\264\265\266\267\270\271\272\273\274\275\276\277'
          '\300\301\302\303\304\305\306\307\310\311\312\313\314\315\316\317'
          '\320\321\322\323\324\325\326\327\330\331\332\333\334\335\336\337'
          '\340\341\342\343\344\345\346\347\350\351\352\353\354\355\356\357'
          '\360\361\362\363\364\365\366\367\370\371\372\373\374\375\376\377';
      `);
      assertEquals(
        val,
        Uint8Array.of(
          0x68,
          0x65,
          0x6c,
          0x6c,
          0x6f,
          0x5c,
          0o000,
          0o001,
          0o002,
          0o003,
          0o004,
          0o005,
          0o006,
          0o007,
          0o010,
          0o011,
          0o012,
          0o013,
          0o014,
          0o015,
          0o016,
          0o017,
          0o020,
          0o021,
          0o022,
          0o023,
          0o024,
          0o025,
          0o026,
          0o027,
          0o030,
          0o031,
          0o032,
          0o033,
          0o034,
          0o035,
          0o036,
          0o037,
          0o040,
          0o041,
          0o042,
          0o043,
          0o044,
          0o045,
          0o046,
          0o047,
          0o050,
          0o051,
          0o052,
          0o053,
          0o054,
          0o055,
          0o056,
          0o057,
          0o060,
          0o061,
          0o062,
          0o063,
          0o064,
          0o065,
          0o066,
          0o067,
          0o070,
          0o071,
          0o072,
          0o073,
          0o074,
          0o075,
          0o076,
          0o077,
          0o100,
          0o101,
          0o102,
          0o103,
          0o104,
          0o105,
          0o106,
          0o107,
          0o110,
          0o111,
          0o112,
          0o113,
          0o114,
          0o115,
          0o116,
          0o117,
          0o120,
          0o121,
          0o122,
          0o123,
          0o124,
          0o125,
          0o126,
          0o127,
          0o130,
          0o131,
          0o132,
          0o133,
          0o134,
          0o135,
          0o136,
          0o137,
          0o140,
          0o141,
          0o142,
          0o143,
          0o144,
          0o145,
          0o146,
          0o147,
          0o150,
          0o151,
          0o152,
          0o153,
          0o154,
          0o155,
          0o156,
          0o157,
          0o160,
          0o161,
          0o162,
          0o163,
          0o164,
          0o165,
          0o166,
          0o167,
          0o170,
          0o171,
          0o172,
          0o173,
          0o174,
          0o175,
          0o176,
          0o177,
          0o200,
          0o201,
          0o202,
          0o203,
          0o204,
          0o205,
          0o206,
          0o207,
          0o210,
          0o211,
          0o212,
          0o213,
          0o214,
          0o215,
          0o216,
          0o217,
          0o220,
          0o221,
          0o222,
          0o223,
          0o224,
          0o225,
          0o226,
          0o227,
          0o230,
          0o231,
          0o232,
          0o233,
          0o234,
          0o235,
          0o236,
          0o237,
          0o240,
          0o241,
          0o242,
          0o243,
          0o244,
          0o245,
          0o246,
          0o247,
          0o250,
          0o251,
          0o252,
          0o253,
          0o254,
          0o255,
          0o256,
          0o257,
          0o260,
          0o261,
          0o262,
          0o263,
          0o264,
          0o265,
          0o266,
          0o267,
          0o270,
          0o271,
          0o272,
          0o273,
          0o274,
          0o275,
          0o276,
          0o277,
          0o300,
          0o301,
          0o302,
          0o303,
          0o304,
          0o305,
          0o306,
          0o307,
          0o310,
          0o311,
          0o312,
          0o313,
          0o314,
          0o315,
          0o316,
          0o317,
          0o320,
          0o321,
          0o322,
          0o323,
          0o324,
          0o325,
          0o326,
          0o327,
          0o330,
          0o331,
          0o332,
          0o333,
          0o334,
          0o335,
          0o336,
          0o337,
          0o340,
          0o341,
          0o342,
          0o343,
          0o344,
          0o345,
          0o346,
          0o347,
          0o350,
          0o351,
          0o352,
          0o353,
          0o354,
          0o355,
          0o356,
          0o357,
          0o360,
          0o361,
          0o362,
          0o363,
          0o364,
          0o365,
          0o366,
          0o367,
          0o370,
          0o371,
          0o372,
          0o373,
          0o374,
          0o375,
          0o376,
          0o377
        )
      );
    } finally {
      await conn.end();
    }
  });

  test('type encode', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      let typeName, text;
      // int4
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1::text`,
        params: [{ type: 'int4', value: 1 }]
      });
      assertEquals(typeName, 'integer');
      assertEquals(text, '1');
      // bool
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1::text`,
        params: [{ type: 'bool', value: true }]
      });
      assertEquals(typeName, 'boolean');
      assertEquals(text, 'true');
      // jsonb
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1->>'key'`,
        params: [{ type: 'jsonb', value: { key: 'hello' } }]
      });
      assertEquals(typeName, 'jsonb');
      assertEquals(text, 'hello');
      // bytea
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, encode($1, 'hex')`,
        params: [{ type: 'bytea', value: Uint8Array.of(0xca, 0xfe, 0xba, 0xbe) }]
      });
      assertEquals(typeName, 'bytea');
      assertEquals(text, 'cafebabe');
      // text[]
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, to_json($1)::text`,
        params: [{ type: 'text[]', value: ['1', '2', '3', ' \t"hello world"\n ', null] }]
      });
      assertEquals(typeName, 'text[]');
      assertEquals(JSON.parse(text), ['1', '2', '3', ' \t"hello world"\n ', null]);
      // bytea[]
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1::text`,
        params: [{ type: 'bytea[]', value: [0xca, 0xfe, 0xba, 0xbe].map((x) => Uint8Array.of(x)) }]
      });
      assertEquals(typeName, 'bytea[]');
      assertEquals(text, '{"\\\\xca","\\\\xfe","\\\\xba","\\\\xbe"}');
      // varchar
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1::text`,
        params: [{ type: 'varchar', value: 'hello' }]
      });
      assertEquals(typeName, 'character varying');
      assertEquals(text, 'hello');
      // oid
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1::text`,
        params: [{ type: 'oid', value: 1 }]
      });
      assertEquals(typeName, 'oid');
      assertEquals(text, '1');
      // null
      [typeName, text] = await conn.query({
        statement: /*sql*/ `select pg_typeof($1)::text, $1::text`,
        params: [{ type: 'text', value: null }]
      });
      assertEquals(typeName, 'text');
      assertEquals(text, null);
    } finally {
      await conn.end();
    }
  });

  test('emoji', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [res] = await conn.query({
        statement: /*sql*/ `select $1`,
        params: [{ type: 'text', value: '\u{1f4ce}' }]
      });
      assertEquals(res, '\u{1f4ce}');
    } finally {
      await conn.end();
    }
  });

  test('negative tableColumn', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const res = await conn.query({
        statement: /*sql*/ `select ctid from pg_type limit 0`
      });
      assertEquals(res.columns[0].tableColumn, -1);
    } finally {
      await conn.end();
    }
  });

  // test('CREATE_REPLICATION_SLOT issue', async () => {
  //   const conn = await pgwire.connect(process.env.POSTGRES, {
  //     replication: 'database',
  //   });
  //   try {
  //     await Promise.all([
  //       conn.query('CREATE_REPLICATION_SLOT crs_iss LOGICAL test_decoding'),
  //       conn.query(/*sql*/ `SELECT 1`),
  //     ]);
  //     await Promise.all([
  //       conn.query('CREATE_REPLICATION_SLOT crs_iss1 LOGICAL test_decoding'),
  //       assert.rejects(conn.query(/*sql*/ `SELECT 1/0`)),
  //       conn.query(/*sql*/ `SELECT 1`),
  //     ]);
  //   } finally {
  //     conn.end();
  //   }
  // });

  _test('logical replication', async (_) => {
    const conn = await pgconnect('postgres://pgwire@pg:5432/postgres?replication=database');
    try {
      await conn.query(/*sql*/ `
        select pg_create_logical_replication_slot('test_2b265aa1', 'test_decoding', temporary:=true)
      `);
      await conn.query(/*sql*/ `create table foo_2b265aa1 as select 1 a`); // generate changes
      const { scalar: currentLsn } = await conn.query(/*sql*/ `select pg_current_wal_lsn()`);
      const { rows: expected } = await conn.query(/*sql*/ `
        select lsn, data from pg_logical_slot_peek_changes('test_2b265aa1', null, null)
      `);
      assertEquals(expected.length, 3);

      const actual = [];
      const utf8dec = new TextDecoder();
      const replstream = conn.logicalReplication({ slot: 'test_2b265aa1' });

      outer: for await (const chunk of replstream)
        for (const { lsn, data } of chunk) {
          actual.push([lsn, utf8dec.decode(data)]);
          if (lsn >= currentLsn) {
            break outer;
          }
        }
      assertEquals(actual, expected);

      // connection should be reusable after replication end
      const { scalar: hello } = await conn.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await conn.end();
    }
  });

  test('logical replication pgoutput', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?replication=database');
    try {
      await Promise.all([
        conn.query(/*sql*/ `create table pgo1rel(id int not null primary key, val text, note text)`),
        conn.query(/*sql*/ `alter table pgo1rel alter column note set storage external`),
        conn.query(/*sql*/ `create publication pgo1pub for table pgo1rel`),
        conn.query(/*sql*/ `select pg_replication_origin_create('pgo1origin')`),
        conn.query(`CREATE_REPLICATION_SLOT pgo1slot TEMPORARY LOGICAL pgoutput`),
        // conn.query(/*sql*/ `select pg_replication_origin_session_setup('pgo1origin')`),
        // generate changes
        conn.query(/*sql*/ `begin`),
        conn.query(/*sql*/ `insert into pgo1rel select 1, 'foo', repeat('_toasted_', 10000)`),
        // toasted column unchanged, after.note == undefined expected
        conn.query(/*sql*/ `update pgo1rel set val = 'bar'`),
        // key changed
        conn.query(/*sql*/ `update pgo1rel set id = 2`),
        conn.query(/*sql*/ `delete from pgo1rel`),
        conn.query(/*sql*/ `alter table pgo1rel replica identity full`),
        conn.query(/*sql*/ `insert into pgo1rel select 1, 'foo', repeat('_toasted_', 10000)`),
        // toasted column unchanged, but replica identity is full, so after.note == '_toasted_....' expected
        conn.query(/*sql*/ `update pgo1rel set val = 'bar'`),
        conn.query(/*sql*/ `delete from pgo1rel`),
        conn.query(/*sql*/ `truncate pgo1rel`),
        conn.query(
          /*sql*/ `select set_config('pgo1test.message_lsn', lsn::text, false) from pg_logical_emit_message(true, 'testmessage', 'hello world'::bytea) lsn`
        ),
        conn.query(/*sql*/ `commit`)
      ]);
      const [stopLsn] = await conn.query(/*sql*/ `select pg_current_wal_lsn()`);
      const [messageLsn] = await conn.query(/*sql*/ `select current_setting('pgo1test.message_lsn')::pg_lsn`);

      const replstream = conn.logicalReplication({
        slot: 'pgo1slot',
        options: {
          proto_version: '1',
          publication_names: 'pgo1pub',
          // binary: 'true',
          messages: 'true'
        }
      });
      const actual = [];
      for await (const chunk of replstream.pgoutputDecode()) {
        // console.log(chunk);
        for (const pgomsg of chunk.messages) {
          actual.push(pgomsg);
        }
        // console.log({ lastLsn: chunk.lastLsn, stopLsn });
        if (chunk.lastLsn >= stopLsn) {
          break;
        }
      }
      // console.log(actual);

      const mbegin = actual.shift();
      assertEquals(typeof mbegin.lsn, 'string');
      assertEquals(typeof mbegin.time, 'bigint');
      assertEquals(mbegin.tag, 'begin');
      assertEquals(typeof mbegin.commitLsn, 'string');
      assertEquals(typeof mbegin.commitTime, 'bigint');
      assertEquals(typeof mbegin.xid, 'number');

      const mrel = actual.shift();
      assertEquals(mrel.lsn, null);
      assertEquals(typeof mrel.time, 'bigint');
      assertEquals(mrel.tag, 'relation');
      assertEquals(typeof mrel.relationOid, 'number');
      assertEquals(mrel.schema, 'public');
      assertEquals(mrel.name, 'pgo1rel');
      assertEquals(mrel.replicaIdentity, 'default');
      assertEquals(mrel.columns, [
        { flags: 1, typeOid: 23, typeMod: -1, typeSchema: null, typeName: null, name: 'id' },
        { flags: 0, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'val' },
        { flags: 0, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'note' }
      ]);

      // insert into pgo1rel select 1, 'foo', repeat('_toasted_', 10000)
      const minsert = actual.shift();
      assertEquals(typeof minsert.lsn, 'string');
      assertEquals(typeof minsert.time, 'bigint');
      assertEquals(minsert.tag, 'insert');
      assertEquals(minsert.relation, mrel);
      assertEquals(minsert.before, null);
      assertEquals(minsert.beforeRaw, null);
      assertEquals(minsert.afterRaw, { __proto__: null, id: '1', val: 'foo', note: '_toasted_'.repeat(10000) });
      assertEquals(minsert.after.id, 1);
      assertEquals(minsert.after.val, 'foo');
      assertEquals(minsert.after.note, '_toasted_'.repeat(10000));

      // update pgo1rel set val = 'bar'
      const mupdate = actual.shift();
      assertEquals(typeof mupdate.lsn, 'string');
      assertEquals(typeof mupdate.time, 'bigint');
      assertEquals(mupdate.tag, 'update');
      assertEquals(mupdate.relation, mrel);
      assertEquals(mupdate.before, null); // key was not changed
      assertEquals(mupdate.beforeRaw, null); // key was not changed
      assertEquals(mupdate.afterRaw, { __proto__: null, id: '1', val: 'bar', note: undefined });
      assertEquals(mupdate.after.id, 1);
      assertEquals(mupdate.after.val, 'bar');
      assertEquals(mupdate.after.note, undefined);

      // update pgo1rel set id = 2
      const mupdate_ = actual.shift();
      assertEquals(typeof mupdate_.lsn, 'string');
      assertEquals(typeof mupdate_.time, 'bigint');
      assertEquals(mupdate_.tag, 'update');
      assertEquals(mupdate_.relation, mrel);
      assertEquals(mupdate_.beforeRaw, { __proto__: null, id: '1', val: undefined, note: undefined });
      assertEquals(mupdate_.before.id, 1);
      assertEquals(mupdate_.before.val, undefined);
      assertEquals(mupdate_.before.note, undefined);
      assertEquals(mupdate_.afterRaw, { __proto__: null, id: '2', val: 'bar', note: undefined });
      assertEquals(mupdate_.after.id, 2);
      assertEquals(mupdate_.after.val, 'bar');
      assertEquals(mupdate_.after.note, undefined);

      // delete from pgo1rel
      const mdelete = actual.shift();
      assertEquals(typeof mdelete.lsn, 'string');
      assertEquals(typeof mdelete.time, 'bigint');
      assertEquals(mdelete.tag, 'delete');
      assertEquals(mdelete.relation, mrel);
      assertEquals(mdelete.beforeRaw, { __proto__: null, id: '2', note: undefined, val: undefined });
      assertEquals(mdelete.before.id, 2);
      assertEquals(mdelete.before.note, undefined);
      assertEquals(mdelete.before.val, undefined);
      assertEquals(mdelete.afterRaw, null);
      assertEquals(mdelete.after, null);

      // alter table pgo1rel replica identity full
      const mrel2 = actual.shift();
      assertEquals(mrel2.lsn, null);
      assertEquals(typeof mrel2.time, 'bigint');
      assertEquals(mrel2.tag, 'relation');
      assertEquals(typeof mrel2.relationOid, 'number');
      assertEquals(mrel2.schema, 'public');
      assertEquals(mrel2.name, 'pgo1rel');
      assertEquals(mrel2.replicaIdentity, 'full');
      assertEquals(mrel2.columns, [
        { flags: 1, typeOid: 23, typeMod: -1, typeSchema: null, typeName: null, name: 'id' },
        { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'val' },
        { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'note' }
      ]);

      const minsert2 = actual.shift();
      assertEquals(typeof minsert2.lsn, 'string');
      assertEquals(typeof minsert2.time, 'bigint');
      assertEquals(minsert2.tag, 'insert');
      assertEquals(minsert2.relation, mrel2);
      assertEquals(minsert2.beforeRaw, null);
      assertEquals(minsert2.before, null);
      assertEquals(minsert2.afterRaw, { __proto__: null, id: '1', val: 'foo', note: '_toasted_'.repeat(10000) });
      assertEquals(minsert2.after.id, 1);
      assertEquals(minsert2.after.val, 'foo');
      assertEquals(minsert2.after.note, '_toasted_'.repeat(10000));

      const mupdate2 = actual.shift();
      assertEquals(typeof mupdate2.lsn, 'string');
      assertEquals(typeof mupdate2.time, 'bigint');
      assertEquals(mupdate2.tag, 'update');
      assertEquals(mupdate2.relation, mrel2);
      assertEquals(mupdate2.beforeRaw, { __proto__: null, id: '1', val: 'foo', note: '_toasted_'.repeat(10000) });
      assertEquals(mupdate2.before.id, 1);
      assertEquals(mupdate2.before.val, 'foo');
      assertEquals(mupdate2.before.note, '_toasted_'.repeat(10000));
      assertEquals(mupdate2.afterRaw, { __proto__: null, id: '1', val: 'bar', note: '_toasted_'.repeat(10000) });
      assertEquals(mupdate2.after.id, 1);
      assertEquals(mupdate2.after.val, 'bar');
      assertEquals(mupdate2.after.note, '_toasted_'.repeat(10000));

      const mdelete2 = actual.shift();
      assertEquals(typeof mdelete2.lsn, 'string');
      assertEquals(typeof mdelete2.time, 'bigint');
      assertEquals(mdelete2.tag, 'delete');
      assertEquals(mdelete2.relation, mrel2);
      assertEquals(mdelete2.beforeRaw, { __proto__: null, id: '1', val: 'bar', note: '_toasted_'.repeat(10000) });
      assertEquals(mdelete2.before.id, 1);
      assertEquals(mdelete2.before.val, 'bar');
      assertEquals(mdelete2.before.note, '_toasted_'.repeat(10000));
      assertEquals(mdelete2.afterRaw, null);
      assertEquals(mdelete2.after, null);

      const mrel3 = actual.shift();
      assertEquals(mrel3.lsn, null);
      assertEquals(typeof mrel3.time, 'bigint');
      assertEquals(mrel3.tag, 'relation');
      assertEquals(typeof mrel3.relationOid, 'number');
      assertEquals(mrel3.schema, 'public');
      assertEquals(mrel3.name, 'pgo1rel');
      assertEquals(mrel3.replicaIdentity, 'full');
      assertEquals(mrel3.columns, [
        { flags: 1, typeOid: 23, typeMod: -1, typeSchema: null, typeName: null, name: 'id' },
        { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'val' },
        { flags: 1, typeOid: 25, typeMod: -1, typeSchema: null, typeName: null, name: 'note' }
      ]);

      const mtruncate = actual.shift();
      assertEquals(typeof mtruncate.lsn, 'string');
      assertEquals(typeof mtruncate.time, 'bigint');
      assertEquals(mtruncate.tag, 'truncate');
      assertEquals(mtruncate.flags, 0);
      assertEquals(mtruncate.cascade, false);
      assertEquals(mtruncate.restartIdentity, false);
      assertEquals(mtruncate.relations, [mrel3]);

      const mmessage = actual.shift();
      assertEquals(typeof mmessage.lsn, 'string');
      assertEquals(typeof mmessage.time, 'bigint');
      assertEquals(mmessage.tag, 'message');
      assertEquals(mmessage.flags, 1);
      assertEquals(mmessage.transactional, true);
      assertEquals(mmessage.messageLsn, messageLsn);
      assertEquals(mmessage.prefix, 'testmessage');
      const helloworld = Uint8Array.from([104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]);
      assertEquals(mmessage.content, helloworld);

      const mcommit = actual.shift();
      assertEquals(typeof mcommit.lsn, 'string');
      assertEquals(typeof mcommit.time, 'bigint');
      assertEquals(mcommit.tag, 'commit');
      assertEquals(mcommit.flags, 0);
      assertEquals(mcommit.commitTime, mbegin.commitTime);
      assertEquals(mcommit.commitLsn, mbegin.commitLsn);
      const mend = actual.shift();
      assertEquals(mend, undefined);

      // connection should be reusable after replication end
      const [hello] = await conn.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await conn.end();
    }
  });

  test('logical replication invalid startLsn', async (_) => {
    let caughtException;
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?replication=database&_debug=0');
    try {
      const replicationStream = conn.logicalReplication({ slot: 'test', startLsn: 'invalid/lsn' });
      for await (const _ of replicationStream);
    } catch (err) {
      caughtException = err;
    } finally {
      await conn.end();
    }
    assertEquals(String(caughtException), 'RangeError: invalid lsn');
  });

  // test('logical replication ack', async () => {
  //   psql(/*sql*/ `
  //     BEGIN;
  //     CREATE TABLE acktest(a INT NOT NULL PRIMARY KEY);
  //     CREATE PUBLICATION acktest FOR TABLE acktest;
  //     COMMIT;

  //     SELECT pg_create_logical_replication_slot('acktest', 'test_decoding');

  //     BEGIN;
  //     INSERT INTO acktest VALUES (1), (2);
  //     COMMIT;
  //     BEGIN;
  //     INSERT INTO acktest VALUES (3), (4);
  //     COMMIT;
  //   `);
  //   const changesCount = JSON.parse(psql(/*sql*/ `
  //     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
  //   `));
  //   assert.deepStrictEqual(changesCount, 8);
  //   const firstCommitLsn = JSON.parse(psql(/*sql*/ `
  //     SELECT to_json(lsn)
  //     FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
  //     WHERE data LIKE 'COMMIT%'
  //     LIMIT 1
  //   `));
  //   const client = pgwire.pool(process.env.POSTGRES);
  //   const replstream = await client.logicalReplication({
  //     slot: 'acktest',
  //     startLsn: '0/0',
  //   });
  //   replstream.ack(firstCommitLsn);
  //   replstream.destroy();
  //   await finished(replstream);
  //   const changesCountAfterAck = JSON.parse(psql(/*sql*/ `
  //     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest', NULL, NULL)
  //   `));
  //   assert.deepStrictEqual(changesCountAfterAck, 4);
  // });

  // test('logical replication ignore ack after destroy', async () => {
  //   psql(/*sql*/ `
  //     BEGIN;
  //     CREATE TABLE acktest_1(a INT NOT NULL PRIMARY KEY);
  //     CREATE PUBLICATION acktest_1 FOR TABLE acktest_1;
  //     COMMIT;

  //     SELECT pg_create_logical_replication_slot('acktest_1', 'test_decoding');

  //     BEGIN;
  //     INSERT INTO acktest_1 VALUES (1), (2);
  //     COMMIT;
  //     BEGIN;
  //     INSERT INTO acktest_1 VALUES (3), (4);
  //     COMMIT;
  //   `);
  //   const changesCount = JSON.parse(psql(/*sql*/ `
  //     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest_1', NULL, NULL)
  //   `));
  //   assert.deepStrictEqual(changesCount, 8);
  //   const [firstCommitLsn, lastCommitLsn] = JSON.parse(psql(/*sql*/ `
  //     SELECT json_agg(lsn)
  //     FROM pg_logical_slot_peek_changes('acktest_1', NULL, NULL)
  //     WHERE data LIKE 'COMMIT%'
  //   `));
  //   const client = pgwire.pool(process.env.POSTGRES);
  //   const replstream = await client.logicalReplication({ slot: 'acktest_1' });
  //   replstream.ack(firstCommitLsn);
  //   replstream.destroy();
  //   replstream.ack(lastCommitLsn);
  //   await finished(replstream);
  //   const changesCountAfterAck = JSON.parse(psql(/*sql*/ `
  //     SELECT count(*) FROM pg_logical_slot_peek_changes('acktest_1', NULL, NULL)
  //   `));
  //   assert.deepStrictEqual(changesCountAfterAck, 4);
  // });

  // test('logical replication invalid startLsn', async () => {
  //   const client = pgwire.pool(process.env.POSTGRES);
  //   const response = client.logicalReplication({
  //     slot: 'unicorn',
  //     startLsn: 'invalid_lsn',
  //   });
  //   await assert.rejects(response, {
  //     code: 'PGERR_INVALID_START_LSN',
  //   });
  // });

  _test('parse bind execute', async (_) => {
    const pg = pgpool('postgres://postgres:secret@pg:5432/postgres');
    const { scalar } = await pg.query(
      {
        message: 'Parse',
        statement: /*sql*/ `SELECT $1`,
        paramTypes: ['int4']
      },
      {
        message: 'Bind',
        params: [
          {
            type: 'int4',
            value: 1
          }
        ]
      },
      {
        message: 'Execute'
      }
    );
    assertEquals(scalar, 1);
  });

  test('connection end', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      assertEquals(conn.queryable, true);
      await conn.query(); // connect
      await Promise.all([
        // connection should be queryable
        conn.query(/*sql*/ `select 'ok'`).then(([health]) => assertEquals(health, 'ok')),
        // enqueue connection end
        conn.end().then((_) => assertEquals(conn.pid, null)),
        // connection expected to be alive
        assertEquals(conn.pid > 0, true),
        // but not queryable
        assertEquals(conn.queryable, false),
        // concurent .end calls should be no-op, but still wait until connection terminated
        conn.end().then((_) => assertEquals(conn.pid, null)),
        // connection should be closed for new queries immediately
        Promise.allSettled([conn.query(/*sql*/ `select 'impossible'`)]).then(([{ status, reason }]) => {
          assertEquals(status, 'rejected');
          assertEquals(String(reason), 'Error: postgres query late');
        })
      ]);
      // connection should still be closed for new queries
      const [res] = await Promise.allSettled([conn.query(/*sql*/ `select 'impossible'`)]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'Error: postgres query late');
    } finally {
      // subsequent .end calls should be no-op
      await conn.end();
    }
  });

  test('pending queries should be rejected when server closes connection', async (_) => {
    // if exit_on_error=on then all errors destroy connection
    // https://www.postgresql.org/docs/16/runtime-config-error-handling.html#GUC-EXIT-ON-ERROR
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?exit_on_error=on');
    try {
      const [res1, res2] = await Promise.allSettled([
        conn.query(/*sql*/ `select 1 / 0`),
        conn.query(/*sql*/ `select 'doomed'`)
      ]);
      assertEquals(res1.status, 'rejected');
      assertEquals(String(res1.reason), 'PgError.22012: division by zero');
      assertEquals(res1.reason.cause.severity, 'FATAL');
      assertEquals(res1.reason.cause.code, '22012');
      assertEquals(res1.reason.cause.message, 'division by zero');

      assertEquals(res2.status, 'rejected');
      assertEquals(String(res2.reason), 'Error: postgres query failed');
    } finally {
      await conn.end();
    }
  });

  test('auth cleartext with bad password', async (_) => {
    const conn = pgconnection('postgres://pgwire_pwd@pg:5432/postgres');
    try {
      const [res] = await Promise.allSettled([conn.query()]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'PgError.28P01: empty password returned by client');
      // assertEquals(res.reason.cause.severity, 'FATAL');
      // assertEquals(res.reason.cause.code, '28P01');
      // assertEquals(res.reason.cause.message, 'empty password returned by client');
    } finally {
      await conn.end();
    }
  });

  test('auth cleartext', async (_) => {
    const conn = pgconnection('postgres://pgwire_pwd:secret@pg:5432/postgres');
    try {
      const [username] = await conn.query(/*sql*/ `select current_user`);
      assertEquals(username, 'pgwire_pwd');
    } finally {
      await conn.end();
    }
  });

  test('auth md5 with bad password', async (_) => {
    const conn = pgconnection('postgres://pgwire_md5@pg:5432/postgres');
    try {
      const [res] = await Promise.allSettled([conn.query()]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'PgError.28P01: password authentication failed for user "pgwire_md5"');
      // assertEquals(res.reason.cause.severity, 'FATAL');
      // assertEquals(res.reason.cause.code, '28P01');
      // assertEquals(res.reason.cause.message, 'password authentication failed for user "pgwire_md5"');
    } finally {
      await conn.end();
    }
  });

  test('auth md5', async (_) => {
    const conn = pgconnection('postgres://pgwire_md5:secret@pg:5432/postgres');
    try {
      const [username] = await conn.query(/*sql*/ `select current_user`);
      assertEquals(username, 'pgwire_md5');
    } finally {
      await conn.end();
    }
  });

  test('auth scram-sha-256 with bad password', async (_) => {
    const conn = pgconnection('postgres://pgwire_sha256@pg:5432/postgres');
    try {
      const [res] = await Promise.allSettled([conn.query()]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'PgError.28P01: password authentication failed for user "pgwire_sha256"');
      // assertEquals(res.reason.cause.severity, 'FATAL');
      // assertEquals(res.reason.cause.code, '28P01');
      // assertEquals(res.reason.cause.message, 'password authentication failed for user "pgwire_sha256"');
    } finally {
      await conn.end();
    }
  });

  test('auth scram-sha-256', async (_) => {
    const conn = pgconnection('postgres://pgwire_sha256:secret@pg:5432/postgres');
    try {
      const [username] = await conn.query(/*sql*/ `select current_user`);
      assertEquals(username, 'pgwire_sha256');
    } finally {
      await conn.end();
    }
  });

  async function get_sslrootcert() {
    const conn1 = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [sslrootcert] = await conn1.query(/*sql*/ `select pg_read_file('ca.crt')`);
      return sslrootcert;
    } finally {
      await conn1.end();
    }
  }

  test('sslmode=require should create encrypted connection if ssl is supported', async (_) => {
    const sslrootcert = await get_sslrootcert();
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres', {
      sslmode: 'require',
      sslrootcert
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, true);
      assertEquals(conn.ssl, true);
    } finally {
      await conn.end();
    }
  });

  test('sslmode=require should reject connection if ssl is not supported', async (_) => {
    const sslrootcert = await get_sslrootcert();
    const conn = pgconnection('postgres://pgwire@pg:6432/postgres', {
      sslmode: 'require',
      sslrootcert
    });
    try {
      const [res] = await Promise.allSettled([conn.query()]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'Error: postgres query failed');
      assertEquals(String(res.reason.cause), 'Error: postgres does not support ssl');
    } finally {
      await conn.end();
    }
  });

  test('sslmode=prefer should create encypted connection if ssl is supported', async (_) => {
    const sslrootcert = await get_sslrootcert();
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres', {
      sslmode: 'prefer',
      sslrootcert
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, true);
      assertEquals(conn.ssl, true);
    } finally {
      await conn.end();
    }
  });

  test('sslmode=prefer should create unencypted connection if ssl is not supported', async (_) => {
    const sslrootcert = await get_sslrootcert();
    const conn = pgconnection('postgres://pgwire@pg:6432/postgres', {
      sslmode: 'prefer',
      sslrootcert
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, false);
      assertEquals(conn.ssl, false);
    } finally {
      await conn.end();
    }
  });

  test('sslmode=prefer should create unencypted connection if ssl is denied', async (_) => {
    const sslrootcert = await get_sslrootcert();
    const conn = pgconnection('postgres://pgwire_nossl@pg:5432/postgres', {
      sslmode: 'prefer',
      sslrootcert
      // _debug: true,
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, false);
      assertEquals(conn.ssl, false);
    } finally {
      await conn.end();
    }
  });

  test('sslmode=prefer should create unencypted connection if ssl handshake fails', async (_) => {
    const conn = pgconnection('postgres://pgwire_nossl@pg:5432/postgres', {
      sslmode: 'prefer',
      sslrootcert: '__invalid_cert__'
      // _debug: true,
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, false);
      assertEquals(conn.ssl, false);
    } finally {
      await conn.end();
    }
  });

  test('sslmode=allow should create unencypted connection if possible', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres', {
      sslmode: 'allow'
      // _debug: true,
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, false);
      assertEquals(conn.ssl, false);
    } finally {
      await conn.end();
    }
  });

  test('sslmode=allow should use ssl if server requires ssl', async (_) => {
    const sslrootcert = await get_sslrootcert();
    const conn = pgconnection('postgres://pgwire_sslonly@pg:5432/postgres', {
      sslmode: 'allow',
      sslrootcert
      // _debug: true,
    });
    try {
      const [sslUsed] = await conn.query(/*sql*/ `select ssl from pg_stat_ssl where pid = pg_backend_pid()`);
      assertEquals(sslUsed, true);
      assertEquals(conn.ssl, true);
    } finally {
      await conn.end();
    }
  });

  test('pool should reuse connection', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=1');
    try {
      const [pid1] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      const [pid2] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      assertEquals(pid1, pid2);
    } finally {
      await pool.end();
    }
  });

  test('pool should do connection per query when poolSize is unset', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres');
    try {
      const [pid] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      assertEquals(typeof pid, 'number');
      const [terminated] = await pool.query(/*sql*/ `select pg_terminate_backend(${pid})`);
      assertEquals(terminated, false);
    } finally {
      await pool.end();
    }
  });

  test('pool should prevent idle in trasaction', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=1');
    try {
      const [res1, res2] = await Promise.allSettled([
        // emit bad query with explicit transaction
        pool.query(/*sql*/ `begin`),
        // then enqueue good innocent query in the same connection as previous query
        pool.query(/*sql*/ `select 1`)
      ]);
      assertEquals(res1.status, 'rejected');
      assertEquals(String(res1.reason), 'Error: postgres query failed');
      assertEquals(String(res1.reason.cause), 'Error: postgres connection left in transaction');
      assertEquals(res2.status, 'rejected');
      assertEquals(String(res2.reason), 'Error: postgres query failed');
      assertEquals(String(res2.reason.cause), 'Error: postgres connection left in transaction');
      // poisoned connection should be already destroyed and forgotten
      // in this event loop iteration and fresh connection should be created
      // so no errors expected
      const [hello] = await pool.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await pool.end();
    }
  });

  // TODO it works, but I'm not sure that I know why.
  // Seems that postgres fails to `commit` because
  // we not consume all backend messages before connection destroy.
  // Can we rely on this behavior?
  _test('pool should auto rollback', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=1&_debug=0');
    try {
      const [res1, res2] = await Promise.allSettled([
        // emit bad query with explicit transaction
        pool.query(/*sql*/ `begin; create table this_table_should_not_be_created();`),
        // then enqueue `commit` in the same connection as previous query
        pool.query(/*sql*/ `commit`)
      ]);

      assertEquals(res1.status, 'rejected');
      assertEquals(String(res1.reason), 'Error: postgres connection left in transaction');
      assertEquals(res2.status, 'rejected');
      assertEquals(String(res2.reason), 'Error: postgres query failed');
      // assertEquals(caughtError2.cause == caughtError1, true);
      // if first query was not rollbacked then next query will fail
      await pool.query(/*sql*/ `create temp table this_table_should_not_be_created()`);
    } finally {
      await pool.end();
    }
  });

  _test('pool should keep reason of left_in_txn', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=1&_debug=0');
    try {
      const [res] = await Promise.allSettled([pool.query(/*sql*/ `begin; select 1 / 0; commit;`)]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'Error: postgres connection left in transaction');
      assertEquals(String(res.reason.cause), 'PgError.22012: division by zero');
    } finally {
      await pool.end();
    }
  });

  // test('pool - unexisting database', async () => {
  //   const pool = pgwire.pool({
  //     database: 'unicorn',
  //   }, process.env.POSTGRES);
  //   try {
  //     await assert.rejects(pool.query(/*sql*/ `SELECT`), {
  //       code: 'PGERR_3D000',
  //     });
  //   } finally {
  //     pool.clear();
  //   }
  // });

  test('pool idle timeout', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=1&_poolIdleTimeout=1s');
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [pid] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      assertEquals(typeof pid, 'number');
      await delay(100);
      const [alive] = await conn.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(alive, true);
      await delay(2000);
      const [stillAlive] = await conn.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(stillAlive, false);
    } finally {
      await pool.end();
      await conn.end();
    }
  });

  test('pool async error', async (_) => {
    // use pool with single connection
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=1');
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      // new pooled connection should be created
      const [pid] = await pool.query(/*sql*/ `select pg_backend_pid()`);
      const [alive] = await conn.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(alive, true);
      // cause pooled connection down,
      await conn.query(/*sql*/ `select pg_terminate_backend(${pid})`);
      await delay(200);
      const [stillAlive] = await conn.query(/*sql*/ `select exists (select from pg_stat_activity where pid = ${pid})`);
      assertEquals(stillAlive, false);
      // pool should be able to execute queries in new connection
      const [hello] = await pool.query(/*sql*/ `select 'hello'`);
      assertEquals(hello, 'hello');
    } finally {
      await pool.end();
      await conn.end();
    }
  });

  test('pool should destroy ephemeral conns when _poolSize=0', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=0');
    const [loid] = await pool.query(/*sql*/ `select lo_from_bytea(0, 'initial')`);
    // assertEquals(typeof loid, 'number'); // TODO register oid type
    const resp = pool.stream(/*sql*/ `
      select lo_put(${loid}, 0, 'started'); commit;
      select lo_put(${loid}, 0, 'completed') from pg_sleep(5);
    `);
    try {
      for await (const _ of resp) {
        pool.destroy();
      }
    } catch (err) {
      // console.error(err);
    }
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [lo] = await conn.query(/*sql*/ `select convert_from(lo_get(${loid}), 'sql_ascii')`);
      // destroyed query should be started but not completed
      assertEquals(lo, 'started');
    } finally {
      await conn.end();
    }
  });

  test('pool end should wait until all connections destroyed', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      const [loid] = await conn.query(/*sql*/ `select lo_from_bytea(0, 'initial')`);
      const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=0');
      const [, [p1], [p2]] = await Promise.all([
        pool.query(/*sql*/ `select lo_put(${loid}, 0, 'completed') from pg_sleep(1)`),
        pool.end().then((_) => conn.query(/*sql*/ `select convert_from(lo_get(${loid}), 'sql_ascii')`)),
        pool.end().then((_) => conn.query(/*sql*/ `select convert_from(lo_get(${loid}), 'sql_ascii')`))
      ]);
      assertEquals(p1, 'completed');
      assertEquals(p2, 'completed');
    } finally {
      await conn.end();
    }
  });

  test('pool should release connection after stream interrupted', async (_) => {
    const pool = pgpool('postgres://pgwire@pg:5432/postgres?_poolSize=0');
    try {
      const stream = pool.stream(/*sql*/ `
        set application_name = 'pool_test';
        commit;
        select 'hello';
      `);
      for await (const { rows } of stream) {
        // break after application_name commited
        if (rows.length) break;
      }

      const [count] = await pool.query(/*sql*/ `
        select count(*)
        from pg_stat_activity
        where application_name = 'pool_test'
      `);
      assertEquals(count, 0n);
    } finally {
      await pool.end();
    }
  });

  test('connection application_name', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?application_name=test');
    try {
      const [appname] = await conn.query(/*sql*/ `show application_name`);
      assertEquals(appname, 'test');
    } finally {
      await conn.end();
    }
  });

  _test('unix socket', async (_) => {
    const conn = await pgwire.connect(process.env.POSTGRES_UNIX);
    try {
      const { scalar } = await conn.query(/*sql*/ `select 'hello'`);
      assert.deepStrictEqual(scalar, 'hello');
    } finally {
      conn.end();
    }
  });

  _test('streaming', async (_) => {
    const conn = await pgconnect('postgres://pgwire@pg:5432/postgres');
    try {
      const resp = conn.query(/*sql*/ `select 'hello' col`);
      for await (const chunk of resp) {
        console.log(chunk);
      }
    } finally {
      await conn.end();
    }
  });

  test('cancel by break', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?_debug=0', {
      'x.state': 'initial'
    });
    try {
      const response = conn.stream(/*sql*/ `
        set x.state = 'started';
        commit;
        select from pg_sleep(10);
        set x.state = 'completed';
      `);
      for await (const _ of response) {
        await delay(100); // make window to set 'x.state'
        break;
      }
      const [state] = await conn.query(/*sql*/ `show x.state`);
      assertEquals(state, 'started');
    } finally {
      await conn.end();
    }
  });

  test('cancel simple by signal', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres', {
      // disable wake timer to test that connection wakes on .abort()
      _wakeInterval: 0,
      'x.state': 'initial'
    });
    try {
      const abortCtl = new AbortController();
      const abortReason = Error('cause we can');
      setTimeout((_) => abortCtl.abort(abortReason), 1000);
      const [res] = await Promise.allSettled([
        conn.query(
          /*sql*/ `
          set x.state = 'started';
          commit;
          select from pg_sleep(10);
          set x.state = 'completed';
        `,
          { signal: abortCtl.signal }
        )
      ]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'Error: postgres query aborted');
      assertEquals(res.reason.cause, abortReason);
      const [state] = await conn.query(/*sql*/ `show x.state`);
      assertEquals(state, 'started');
    } finally {
      await conn.end();
    }
  });

  test('cancel extended by signal', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres', {
      // disable wake timer to test that connection wakes on .abort()
      _wakeInterval: 0,
      'x.state': 'initial'
    });
    try {
      const abortCtl = new AbortController();
      const abortReason = Error('cause we can');
      setTimeout((_) => abortCtl.abort(abortReason), 1000);
      const [res] = await Promise.allSettled([
        conn.query(
          { statement: /*sql*/ `set x.state = 'started'` },
          { statement: /*sql*/ `commit` },
          { statement: /*sql*/ `select from pg_sleep(10)` },
          { statement: /*sql*/ `set x.state = 'completed'` },
          { signal: abortCtl.signal }
        )
      ]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'Error: postgres query aborted');
      assertEquals(res.reason.cause, abortReason);
      const [state] = await conn.query(/*sql*/ `show x.state`);
      assertEquals(state, 'started');
    } finally {
      await conn.end();
    }
  });

  _test('wake', async (_) => {
    const conn = await pgconnect('postgres://pgwire@pg:5432/postgres', {
      _wakeInterval: 2,
      _debug: 0
    });
    console.log('.');
    try {
      const stream = conn.stream(
        { statement: /*sql*/ `prepare q as select statement_timestamp() from pg_sleep(1.5)` },
        { statement: /*sql*/ `execute q;` },
        { message: 'Flush' },
        { statement: /*sql*/ `execute q;` },
        { message: 'Flush' },
        { statement: /*sql*/ `execute q;` },
        { message: 'Flush' },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { message: 'Flush' },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` },
        { statement: /*sql*/ `execute q;` }
      );
      let i = 0;
      console.time('chunk');
      for await (const chunk of stream) {
        console.timeLog('chunk', chunk.tag, chunk.rows);
      }
    } finally {
      await conn.end();
    }
  });

  _test('wake2', async (_) => {
    const conn = await pgconnect('postgres://pgwire@pg:5432/postgres', {
      _wakeInterval: 2,
      _debug: 0
    });
    console.log('.');
    try {
      const stream = conn.stream(/*sql*/ `
        select i
        from generate_series(0, 9999999) i
      `);
      let i = 0;
      for await (const chunk of stream) {
        if (chunk.tag == 'wake') {
          console.log('wake', i);
        }
        i++;
      }
      console.log(i);
    } finally {
      await conn.end();
    }
  });

  test('notifications', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?_debug=0');
    try {
      const actual = [];
      const [pid] = await conn.query(/*sql*/ `select pg_backend_pid()`);
      conn.onnotification = (n) => actual.push(n);
      await conn.query(/*sql*/ `listen test_chan`);
      await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello1')`);
      await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello2')`);
      await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello3')`);
      assertEquals(actual, [
        { pid, channel: 'test_chan', payload: 'hello1' },
        { pid, channel: 'test_chan', payload: 'hello2' },
        { pid, channel: 'test_chan', payload: 'hello3' }
      ]);
    } finally {
      await conn.end();
    }
  });

  _test('notifications handler should not swallow errors', async (_) => {
    const conn = await pgconnect('postgres://pgwire@pg:5432/postgres?_debug=1');
    try {
      conn.onnotification = onnotification;
      await conn.query(/*sql*/ `listen test_chan`);
      await conn.query(/*sql*/ `select pg_notify('test_chan', 'hello1')`);
    } finally {
      await conn.end();
    }
    function onnotification(n) {
      throw Error('boom');
    }
  });

  test('connectRetry', async (_) => {
    await Promise.all([client(), server()]);

    async function client() {
      const conn = pgconnection('postgres://pgwire@pg:5433/postgres?_connectRetry=10s&_debug=0');
      try {
        const [health] = await conn.query(/*sql*/ `select 'ok'`);
        assertEquals(health, 'ok');
      } finally {
        await conn.end();
      }
    }
    async function server() {
      const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
      try {
        await conn.query(String.raw/*sql*/ `
          select pg_sleep(4);
          set statement_timeout = '10s';
          create temp table _out (ln text);
          copy _out from program 'socat TCP4-LISTEN:5433 TCP4:127.0.0.1:5432'
        `);
      } finally {
        await conn.end();
      }
    }
  });

  _test('idle_session_timeout', async (_) => {
    const conn = await pgconnect('postgres://pgwire@pg:5432/postgres?idle_session_timeout=2s&_debug=1');
    try {
      const [health0] = await conn.query(/*sql*/ `select 'ok'`);
      await delay(10_000);
      const [health1] = await conn.query(/*sql*/ `select 'ok'`);
    } finally {
      await conn.end();
    }
  });

  test('maxReadBuf', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres?_maxReadBuf=5242880');
    try {
      const [res] = await Promise.allSettled([conn.query(/*sql*/ `select repeat('a', 10 << 20)`)]);
      assertEquals(res.status, 'rejected');
      assertEquals(String(res.reason), 'Error: postgres query failed');
      assertEquals(String(res.reason.cause), 'Error: postgres sent too big message');
    } finally {
      await conn.end();
    }
  });

  _test('pgbouncer async terminate', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:6432/postgres?_debug=1');
    await Promise.all([
      conn.query(/*sql*/ `select 'hello';`),
      conn.end()
      // conn.whenDestroyed,
    ]);
  });

  test('conn.inTransaction', async (_) => {
    const conn = pgconnection('postgres://pgwire@pg:5432/postgres');
    try {
      assertEquals(conn.inTransaction, 0);
      await conn.query(/*sql*/ `select`);
      assertEquals(conn.inTransaction, 0);
      await conn.query(/*sql*/ `begin`);
      assertEquals(conn.inTransaction, 0x54); // T
      await Promise.allSettled([conn.query(/*sql*/ `select 1 / 0`)]);
      assertEquals(conn.inTransaction, 0x45); // E
      await conn.query(/*sql*/ `rollback`);
      assertEquals(conn.inTransaction, 0);
    } finally {
      await conn.end();
    }
    assertEquals(conn.inTransaction, 0);
  });

  test('unix socket', async (_) => {
    const conn = pgconnection(
      'postgres://?host=/run/postgresql&port=5432&user=pgwire&database=postgres&x.state=initial'
    );
    try {
      const response = conn.stream(/*sql*/ `
        begin;
        set x.state = 'started';
        commit;
        do $$ begin raise notice 'flush'; end $$;
        select from pg_sleep(10);
        set x.state = 'completed';
      `);
      let commitReceived = false;
      for await (const { tag, payload } of response) {
        if (tag == 'CommandComplete' && payload == 'COMMIT') {
          commitReceived = true;
          break;
        }
      }
      assertEquals(commitReceived, true);
      const [state] = await conn.query(/*sql*/ `show x.state`);
      assertEquals(state, 'started'); // but not completed
    } finally {
      await conn.end();
    }
  });

  test('override undefined options', async (_) => {
    const conn = pgconnection(
      { 'x.foo': undefined, 'x.bar': 'world' },
      'postgres://pgwire@pg:5432/postgres?x.foo=hello&x.bar=bang'
    );
    let actual;
    try {
      [actual] = await conn.query(/*sql*/ `
        select format('%s %s', current_setting('x.foo'), current_setting('x.bar'))
      `);
    } finally {
      await conn.end();
    }
    assertEquals(actual, 'hello world');
  });
}

// mute
function _test() {}

async function delay(duration) {
  await new Promise((resolve) => setTimeout(resolve, duration));
}
