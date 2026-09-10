import { mongo } from '@powersync/lib-service-mongodb';
import { ExpressionType } from '@powersync/service-sync-rules';
import { TableSchema } from '@powersync/service-types';

// Match the types exposed after BSON deserialization and conversion to sync rules values.
const BSON_TYPES: Record<string, { name: string; sqliteType: ExpressionType }> = {
  int: { name: 'Integer', sqliteType: ExpressionType.INTEGER },
  long: { name: 'Long', sqliteType: ExpressionType.INTEGER },
  double: { name: 'Double', sqliteType: ExpressionType.REAL },
  decimal: { name: 'Decimal', sqliteType: ExpressionType.TEXT },
  string: { name: 'String', sqliteType: ExpressionType.TEXT },
  symbol: { name: 'String', sqliteType: ExpressionType.TEXT },
  bool: { name: 'Boolean', sqliteType: ExpressionType.INTEGER },
  objectId: { name: 'ObjectId', sqliteType: ExpressionType.TEXT },
  uuid: { name: 'UUID', sqliteType: ExpressionType.TEXT },
  binData: { name: 'Binary', sqliteType: ExpressionType.BLOB },
  date: { name: 'Date', sqliteType: ExpressionType.TEXT },
  timestamp: { name: 'Timestamp', sqliteType: ExpressionType.INTEGER },
  regex: { name: 'RegExp', sqliteType: ExpressionType.TEXT },
  object: { name: 'Object', sqliteType: ExpressionType.TEXT },
  array: { name: 'Array', sqliteType: ExpressionType.TEXT },
  javascript: { name: 'Object', sqliteType: ExpressionType.TEXT },
  javascriptWithScope: { name: 'Object', sqliteType: ExpressionType.TEXT },
  dbPointer: { name: 'Object', sqliteType: ExpressionType.TEXT },
  null: { name: 'Null', sqliteType: ExpressionType.NONE },
  undefined: { name: 'Null', sqliteType: ExpressionType.NONE },
  minKey: { name: 'MinKey', sqliteType: ExpressionType.NONE },
  maxKey: { name: 'MaxKey', sqliteType: ExpressionType.NONE }
};

/**
 * Infer top-level fields without transferring sampled document values to the service.
 * Memory on the service scales with the inferred schema, not the size of those values.
 */
export async function inferCollectionSchema(
  collection: mongo.Collection,
  isDocumentDb: boolean
): Promise<TableSchema['columns']> {
  const fields = await collection
    .aggregate<{ _id: string; types: string[] }>(
      [
        // Keep this first so MongoDB can use its random cursor on large collections.
        { $sample: { size: 50 } },
        {
          $project: {
            _id: 0,
            fields: {
              $map: {
                input: { $objectToArray: '$$ROOT' },
                as: 'field',
                in: {
                  name: '$$field.k',
                  type: {
                    $let: {
                      vars: { bsonType: { $type: '$$field.v' } },
                      in: {
                        $switch: {
                          branches: [
                            {
                              case: { $eq: ['$$bsonType', 'double'] },
                              // Whole-number doubles are replicated as SQLite integers.
                              then: { $cond: [{ $eq: [{ $mod: ['$$field.v', 1] }, 0] }, 'int', 'double'] }
                            },
                            {
                              case: { $eq: ['$$bsonType', 'binData'] },
                              then: {
                                $cond: [
                                  // BinData compares by length, then subtype, then bytes. Only
                                  // 16-byte values of subtype 4 fall within this UUID range.
                                  // This works without transferring binary values or requiring
                                  // the newer MongoDB binary conversion operators.
                                  // https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/#bindata
                                  {
                                    $and: [
                                      { $gte: ['$$field.v', new mongo.UUID('00000000-0000-0000-0000-000000000000')] },
                                      { $lte: ['$$field.v', new mongo.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')] }
                                    ]
                                  },
                                  'uuid',
                                  'binData'
                                ]
                              }
                            }
                          ],
                          default: '$$bsonType'
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        },
        // Discard values before unwinding so large documents aren't duplicated per field.
        { $unwind: '$fields' },
        { $group: { _id: '$fields.name', types: { $addToSet: '$fields.type' } } }
      ],
      {
        // Bound execution per collection below the default 60-second socket timeout
        // so MongoDB can return a query timeout before the connection times out.
        maxTimeMS: 30_000,
        // Small collections can require $sample to sort full documents. Disable
        // disk spill explicitly so concurrent schema queries fail at the memory
        // limit without adding temporary-file I/O on the source database.
        allowDiskUse: false,
        // Field names are case-sensitive even when the collection's default collation isn't.
        // DocumentDB rejects the collation option, including simple collation.
        ...(isDocumentDb ? {} : { collation: { locale: 'simple' } })
      }
    )
    .toArray();

  return fields
    .map(({ _id: name, types }) => {
      let sqliteType = ExpressionType.NONE;
      const bsonTypes = new Set<string>();
      for (const type of types) {
        const inferred = BSON_TYPES[type];
        sqliteType = sqliteType.or(inferred.sqliteType);
        bsonTypes.add(inferred.name);
      }
      const internal_type = [...bsonTypes].sort().join(' | ');
      return { name, type: internal_type, sqlite_type: sqliteType.typeFlags, internal_type, pg_type: internal_type };
    })
    .sort((a, b) => a.name.localeCompare(b.name));
}
