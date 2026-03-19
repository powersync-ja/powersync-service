import { describe, expect, test } from 'vitest';
import {
  DEFAULT_TAG,
  DartSchemaGenerator,
  DotNetClassSchemaGenerator,
  DotNetSchemaGenerator,
  JsLegacySchemaGenerator,
  KotlinSchemaGenerator,
  RoomSchemaGenerator,
  SqlSyncRules,
  StaticSchema,
  SwiftSchemaGenerator,
  TsSchemaGenerator,
  driftSchemaGenerator,
  sqlDelightSchemaGenerator
} from '../../src/index.js';

import { PARSE_OPTIONS } from './util.js';

describe('schema generation', () => {
  const schema = new StaticSchema([
    {
      tag: DEFAULT_TAG,
      schemas: [
        {
          name: 'test_schema',
          tables: [
            {
              name: 'assets',
              columns: [
                { name: 'id', sqlite_type: 'text', internal_type: 'uuid' },
                { name: 'name', sqlite_type: 'text', internal_type: 'text' },
                { name: 'count', sqlite_type: 'integer', internal_type: 'int4' },
                { name: 'owner_id', sqlite_type: 'text', internal_type: 'uuid' }
              ]
            }
          ]
        }
      ]
    }
  ]);

  const { config: rules } = SqlSyncRules.fromYaml(
    `
config:
  edition: 3

streams:
  assets_one:
    query: SELECT * FROM assets as assets1
  assets_2:
    queries:
      - SELECT id, name, count FROM assets as assets2 WHERE name = subscription.parameter('name')
      - SELECT id, owner_id as other_id, foo FROM assets as ASSETS2
`,
    PARSE_OPTIONS
  );

  test('dart', () => {
    const streams = `
extension type TypedSyncStreams(PowerSyncDatabase _db) {
  SyncStream assetsOne() => _db.syncStream('assets_one', {});
  SyncStream assets2({required String name}) => _db.syncStream('assets_2', {'name': name,});
}`;

    expect(new DartSchemaGenerator().generate(rules, schema)).toEqual(`Schema([
  Table('assets1', [
    Column.text('name'),
    Column.integer('count'),
    Column.text('owner_id')
  ]),
  Table('assets2', [
    Column.text('name'),
    Column.integer('count'),
    Column.text('other_id'),
    Column.text('foo')
  ])
]);
${streams}
`);

    expect(new DartSchemaGenerator().generate(rules, schema, { includeTypeComments: true })).toEqual(`Schema([
  Table('assets1', [
    Column.text('name'), // text
    Column.integer('count'), // int4
    Column.text('owner_id') // uuid
  ]),
  Table('assets2', [
    Column.text('name'), // text
    Column.integer('count'), // int4
    Column.text('other_id'), // uuid
    Column.text('foo')
  ])
]);
${streams}
`);
  });

  test('js legacy', () => {
    expect(new JsLegacySchemaGenerator().generate(rules, schema)).toEqual(`new Schema([
  new Table({
    name: 'assets1',
    columns: [
      new Column({ name: 'name', type: ColumnType.TEXT }),
      new Column({ name: 'count', type: ColumnType.INTEGER }),
      new Column({ name: 'owner_id', type: ColumnType.TEXT })
    ]
  }),
  new Table({
    name: 'assets2',
    columns: [
      new Column({ name: 'name', type: ColumnType.TEXT }),
      new Column({ name: 'count', type: ColumnType.INTEGER }),
      new Column({ name: 'other_id', type: ColumnType.TEXT }),
      new Column({ name: 'foo', type: ColumnType.TEXT })
    ]
  })
])
`);
  });

  test('ts', () => {
    expect(new TsSchemaGenerator().generate(rules, schema, {})).toEqual(
      `import { column, Schema, Table, PowerSyncDatabase, SyncStream } from '@powersync/web';
// OR: import { column, Schema, Table, PowerSyncDatabase, SyncStream } from '@powersync/react-native';

const assets1 = new Table(
  {
    // id column (text) is automatically included
    name: column.text,
    count: column.integer,
    owner_id: column.text
  },
  { indexes: {} }
);

const assets2 = new Table(
  {
    // id column (text) is automatically included
    name: column.text,
    count: column.integer,
    other_id: column.text,
    foo: column.text
  },
  { indexes: {} }
);

export const AppSchema = new Schema({
  assets1,
  assets2
});

export type Database = (typeof AppSchema)['types'];

export function typedStreams(db: PowerSyncDatabase) {
  return {
    assetsOne(): SyncStream {
      return db.syncStream('assets_one', {});
    },
    assets2(params: { name: string }): SyncStream {
      return db.syncStream('assets_2', params);
    }
  };
}
`
    );

    expect(new TsSchemaGenerator().generate(rules, schema, { includeTypeComments: true })).toEqual(
      `import { column, Schema, Table, PowerSyncDatabase, SyncStream } from '@powersync/web';
// OR: import { column, Schema, Table, PowerSyncDatabase, SyncStream } from '@powersync/react-native';

const assets1 = new Table(
  {
    // id column (text) is automatically included
    name: column.text, // text
    count: column.integer, // int4
    owner_id: column.text // uuid
  },
  { indexes: {} }
);

const assets2 = new Table(
  {
    // id column (text) is automatically included
    name: column.text, // text
    count: column.integer, // int4
    other_id: column.text, // uuid
    foo: column.text
  },
  { indexes: {} }
);

export const AppSchema = new Schema({
  assets1,
  assets2
});

export type Database = (typeof AppSchema)['types'];

export function typedStreams(db: PowerSyncDatabase) {
  return {
    assetsOne(): SyncStream {
      return db.syncStream('assets_one', {});
    },
    assets2(params: { name: string }): SyncStream {
      return db.syncStream('assets_2', params);
    }
  };
}
`
    );
  });

  test('kotlin', () => {
    const streams = `
@JvmInline
value class TypedSyncStreams(private val db: PowerSyncDatabase) {
  fun assetsOne(): SyncStream = db.syncStream("assets_one", emptyMap())
  fun assets2(name: String): SyncStream = db.syncStream("assets_2", buildMap {
    put("name", JsonParam.String(name))
  })
}`;

    expect(new KotlinSchemaGenerator().generate(rules, schema)).toEqual(`import com.powersync.db.schema.Column
import com.powersync.db.schema.Schema
import com.powersync.db.schema.Table
import com.powersync.PowerSyncDatabase
import com.powersync.sync.SyncStream
import com.powersync.utils.JsonParam
import kotlin.jvm.JvmInline

val schema = Schema(
  Table(
    name = "assets1",
    columns = listOf(
        Column.text("name"),
        Column.integer("count"),
        Column.text("owner_id")
    )
  ),
  Table(
    name = "assets2",
    columns = listOf(
        Column.text("name"),
        Column.integer("count"),
        Column.text("other_id"),
        Column.text("foo")
    )
  )
)
${streams}
`);

    expect(new KotlinSchemaGenerator().generate(rules, schema, { includeTypeComments: true }))
      .toEqual(`import com.powersync.db.schema.Column
import com.powersync.db.schema.Schema
import com.powersync.db.schema.Table
import com.powersync.PowerSyncDatabase
import com.powersync.sync.SyncStream
import com.powersync.utils.JsonParam
import kotlin.jvm.JvmInline

val schema = Schema(
  Table(
    name = "assets1",
    columns = listOf(
        Column.text("name"), // text
        Column.integer("count"), // int4
        Column.text("owner_id") // uuid
    )
  ),
  Table(
    name = "assets2",
    columns = listOf(
        Column.text("name"), // text
        Column.integer("count"), // int4
        Column.text("other_id"), // uuid
        Column.text("foo")
    )
  )
)
${streams}
`);
  });

  test('room', () => {
    expect(new RoomSchemaGenerator().generate(rules, schema)).toEqual(`import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "assets1")
data class Assets1(
  @PrimaryKey val id: String,
  @ColumnInfo("name") val name: String,
  @ColumnInfo("count") val count: Long,
  @ColumnInfo("owner_id") val ownerId: String,
)

@Entity(tableName = "assets2")
data class Assets2(
  @PrimaryKey val id: String,
  @ColumnInfo("name") val name: String,
  @ColumnInfo("count") val count: Long,
  @ColumnInfo("other_id") val otherId: String,
  @ColumnInfo("foo") val foo: String,
)
`);
  });

  test('swift', () => {
    expect(new SwiftSchemaGenerator().generate(rules, schema)).toEqual(`import PowerSync

let schema = Schema(
  Table(
    name: "assets1",
    columns: [
        .text("name"),
        .integer("count"),
        .text("owner_id")
    ]
  ),
  Table(
    name: "assets2",
    columns: [
        .text("name"),
        .integer("count"),
        .text("other_id"),
        .text("foo")
    ]
  )
)

struct TypedSyncStreams {
    private var db: PowerSyncDatabaseProtocol
    init(_ db: PowerSyncDatabaseProtocol) {
        self.db = db
    }
    func assetsOne() -> SyncStream {
        return db.syncStream(name: "assets_one", params: [:])
    }
    func assets2(name: String) -> SyncStream {
        return db.syncStream(name: "assets_2", params: [
            "name": JsonValue.string(name)
        ])
    }
}
`);

    expect(new SwiftSchemaGenerator().generate(rules, schema, { includeTypeComments: true })).toEqual(`import PowerSync

let schema = Schema(
  Table(
    name: "assets1",
    columns: [
        .text("name"), // text
        .integer("count"), // int4
        .text("owner_id") // uuid
    ]
  ),
  Table(
    name: "assets2",
    columns: [
        .text("name"), // text
        .integer("count"), // int4
        .text("other_id"), // uuid
        .text("foo")
    ]
  )
)

struct TypedSyncStreams {
    private var db: PowerSyncDatabaseProtocol
    init(_ db: PowerSyncDatabaseProtocol) {
        self.db = db
    }
    func assetsOne() -> SyncStream {
        return db.syncStream(name: "assets_one", params: [:])
    }
    func assets2(name: String) -> SyncStream {
        return db.syncStream(name: "assets_2", params: [
            "name": JsonValue.string(name)
        ])
    }
}
`);
  });

  test('dotnet', () => {
    expect(new DotNetSchemaGenerator().generate(rules, schema)).toEqual(`using PowerSync.Common.DB.Schema;

class AppSchema
{
    public static Table Assets1 = new Table
    {
        Name = "assets1",
        Columns =
        {
            ["name"] = ColumnType.Text,
            ["count"] = ColumnType.Integer,
            ["owner_id"] = ColumnType.Text,
        },
    };

    public static Table Assets2 = new Table
    {
        Name = "assets2",
        Columns =
        {
            ["name"] = ColumnType.Text,
            ["count"] = ColumnType.Integer,
            ["other_id"] = ColumnType.Text,
            ["foo"] = ColumnType.Text,
        },
    };

    public static Schema PowerSyncSchema = new Schema(Assets1, Assets2);
}

public readonly ref struct TypedSyncStreams(PowerSyncDatabase db)
{
    private PowerSyncDatabase db { get; } = db;
    public ISyncStream AssetsOne()
    {
        var parameters = new Dictionary<string, object>() {};
        return db.SyncStream("assets_one", parameters);
    }
    public ISyncStream Assets2(string name)
    {
        var parameters = new Dictionary<string, object>() {
            { "name", name }
        };
        return db.SyncStream("assets_2", parameters);
    }
}
`);

    expect(new DotNetSchemaGenerator().generate(rules, schema, { includeTypeComments: true }))
      .toEqual(`using PowerSync.Common.DB.Schema;

class AppSchema
{
    public static Table Assets1 = new Table
    {
        Name = "assets1",
        Columns =
        {
            ["name"] = ColumnType.Text, // text
            ["count"] = ColumnType.Integer, // int4
            ["owner_id"] = ColumnType.Text, // uuid
        },
    };

    public static Table Assets2 = new Table
    {
        Name = "assets2",
        Columns =
        {
            ["name"] = ColumnType.Text, // text
            ["count"] = ColumnType.Integer, // int4
            ["other_id"] = ColumnType.Text, // uuid
            ["foo"] = ColumnType.Text,
        },
    };

    public static Schema PowerSyncSchema = new Schema(Assets1, Assets2);
}

public readonly ref struct TypedSyncStreams(PowerSyncDatabase db)
{
    private PowerSyncDatabase db { get; } = db;
    public ISyncStream AssetsOne()
    {
        var parameters = new Dictionary<string, object>() {};
        return db.SyncStream("assets_one", parameters);
    }
    public ISyncStream Assets2(string name)
    {
        var parameters = new Dictionary<string, object>() {
            { "name", name }
        };
        return db.SyncStream("assets_2", parameters);
    }
}
`);
  });

  test('dotnet classes', () => {
    expect(new DotNetClassSchemaGenerator().generate(rules, schema)).toEqual(`using PowerSync.Common.DB.Schema;
using PowerSync.Common.DB.Schema.Attributes;

[Table("assets1")]
public class Assets1Item
{
    // An "id" property is required when using a class-based schema.
    [Column("id")]
    public string Id { get; set; }

    [Column("name")]
    public string Name { get; set; }

    [Column("count")]
    public int Count { get; set; }

    [Column("owner_id")]
    public string OwnerId { get; set; }
}

[Table("assets2")]
public class Assets2Item
{
    // An "id" property is required when using a class-based schema.
    [Column("id")]
    public string Id { get; set; }

    [Column("name")]
    public string Name { get; set; }

    [Column("count")]
    public int Count { get; set; }

    [Column("other_id")]
    public string OtherId { get; set; }

    [Column("foo")]
    public string Foo { get; set; }
}

public class AppSchema
{
    public static Schema PowerSyncSchema = new Schema(typeof(Assets1Item), typeof(Assets2Item));
}

public readonly ref struct TypedSyncStreams(PowerSyncDatabase db)
{
    private PowerSyncDatabase db { get; } = db;
    public ISyncStream AssetsOne()
    {
        var parameters = new Dictionary<string, object>() {};
        return db.SyncStream("assets_one", parameters);
    }
    public ISyncStream Assets2(string name)
    {
        var parameters = new Dictionary<string, object>() {
            { "name", name }
        };
        return db.SyncStream("assets_2", parameters);
    }
}
`);

    expect(new DotNetClassSchemaGenerator().generate(rules, schema, { includeTypeComments: true }))
      .toEqual(`using PowerSync.Common.DB.Schema;
using PowerSync.Common.DB.Schema.Attributes;

[Table("assets1")]
public class Assets1Item
{
    // An "id" property is required when using a class-based schema.
    [Column("id")]
    public string Id { get; set; }

    [Column("name")]
    public string Name { get; set; } // text

    [Column("count")]
    public int Count { get; set; } // int4

    [Column("owner_id")]
    public string OwnerId { get; set; } // uuid
}

[Table("assets2")]
public class Assets2Item
{
    // An "id" property is required when using a class-based schema.
    [Column("id")]
    public string Id { get; set; }

    [Column("name")]
    public string Name { get; set; } // text

    [Column("count")]
    public int Count { get; set; } // int4

    [Column("other_id")]
    public string OtherId { get; set; } // uuid

    [Column("foo")]
    public string Foo { get; set; }
}

public class AppSchema
{
    public static Schema PowerSyncSchema = new Schema(typeof(Assets1Item), typeof(Assets2Item));
}

public readonly ref struct TypedSyncStreams(PowerSyncDatabase db)
{
    private PowerSyncDatabase db { get; } = db;
    public ISyncStream AssetsOne()
    {
        var parameters = new Dictionary<string, object>() {};
        return db.SyncStream("assets_one", parameters);
    }
    public ISyncStream Assets2(string name)
    {
        var parameters = new Dictionary<string, object>() {
            { "name", name }
        };
        return db.SyncStream("assets_2", parameters);
    }
}
`);
  });

  describe('sql', () => {
    const expected = `-- Note: These definitions are only used to generate typed code. PowerSync manages the database schema.
CREATE TABLE assets1(
  id TEXT NOT NULL PRIMARY KEY,
  name TEXT,
  count INTEGER,
  owner_id TEXT
);
CREATE TABLE assets2(
  id TEXT NOT NULL PRIMARY KEY,
  name TEXT,
  count INTEGER,
  other_id TEXT,
  foo TEXT
);
`;

    test('drift', () => {
      expect(driftSchemaGenerator.generate(rules, schema)).toEqual(expected);
    });

    test('sqldelight', () => {
      expect(sqlDelightSchemaGenerator.generate(rules, schema)).toEqual(expected);
    });
  });
});
