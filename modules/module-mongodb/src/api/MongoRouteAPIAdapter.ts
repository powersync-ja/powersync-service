import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { api, ParseSyncRulesOptions, ReplicationHeadCallback, SourceTable } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';

import { MongoManager } from '../replication/MongoManager.js';
import { constructAfterRecord, STANDALONE_CHECKPOINT_ID } from '../replication/MongoRelation.js';
import { CHECKPOINTS_COLLECTION } from '../replication/replication-utils.js';
import * as types from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { MongoLSN } from '../common/MongoLSN.js';

export class MongoRouteAPIAdapter implements api.RouteAPI {
  protected client: mongo.MongoClient;
  public db: mongo.Db;

  connectionTag: string;
  defaultSchema: string;

  constructor(protected config: types.ResolvedConnectionConfig) {
    const manager = new MongoManager(config);
    this.client = manager.client;
    this.db = manager.db;
    this.defaultSchema = manager.db.databaseName;
    this.connectionTag = config.tag ?? sync_rules.DEFAULT_TAG;
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      defaultSchema: this.defaultSchema
    };
  }

  async shutdown(): Promise<void> {
    await this.client.close();
  }

  async [Symbol.asyncDispose]() {
    await this.shutdown();
  }

  async getSourceConfig(): Promise<service_types.configFile.ResolvedDataSourceConfig> {
    return this.config;
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config.id,
      uri: lib_mongo.baseUri(this.config)
    };

    try {
      await this.client.connect();
      await this.db.command({ hello: 1 });
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }
    return {
      ...base,
      connected: true,
      errors: []
    };
  }

  async executeQuery(query: string, params: any[]): Promise<service_types.internal_routes.ExecuteSqlResponse> {
    return service_types.internal_routes.ExecuteSqlResponse.encode({
      results: {
        columns: [],
        rows: []
      },
      success: false,
      error: 'SQL querying is not supported for MongoDB'
    });
  }

  async getDebugTablesInfo(
    tablePatterns: sync_rules.TablePattern[],
    sqlSyncRules: sync_rules.BaseSyncConfig
  ): Promise<api.PatternResult[]> {
    let result: api.PatternResult[] = [];

    const validatePostImages = (schema: string, collection: mongo.CollectionInfo): service_types.ReplicationError[] => {
      if (this.config.postImages == types.PostImagesOption.OFF) {
        return [];
      } else if (!collection.options?.changeStreamPreAndPostImages?.enabled) {
        if (this.config.postImages == types.PostImagesOption.READ_ONLY) {
          return [
            { level: 'fatal', message: `changeStreamPreAndPostImages not enabled on ${schema}.${collection.name}` }
          ];
        } else {
          return [
            {
              level: 'warning',
              message: `changeStreamPreAndPostImages not enabled on ${schema}.${collection.name}, will be enabled automatically`
            }
          ];
        }
      } else {
        return [];
      }
    };

    for (let tablePattern of tablePatterns) {
      const schema = tablePattern.schema;

      let patternResult: api.PatternResult = {
        schema: schema,
        pattern: tablePattern.tablePattern,
        wildcard: tablePattern.isWildcard
      };
      result.push(patternResult);

      let nameFilter: RegExp | string;
      if (tablePattern.isWildcard) {
        nameFilter = new RegExp('^' + escapeRegExp(tablePattern.tablePrefix));
      } else {
        nameFilter = tablePattern.name;
      }

      // Check if the collection exists
      const collections = await this.client
        .db(schema)
        .listCollections(
          {
            name: nameFilter
          },
          { nameOnly: false }
        )
        .toArray();

      if (tablePattern.isWildcard) {
        patternResult.tables = [];
        for (let collection of collections) {
          const sourceTable = new SourceTable({
            id: 0,
            connectionTag: this.connectionTag,
            objectId: collection.name,
            schema: schema,
            name: collection.name,
            replicaIdColumns: [],
            snapshotComplete: true
          });
          let errors: service_types.ReplicationError[] = [];
          if (collection.type == 'view') {
            errors.push({ level: 'warning', message: `Collection ${schema}.${tablePattern.name} is a view` });
          } else {
            errors.push(...validatePostImages(schema, collection));
          }
          const syncData = sqlSyncRules.tableSyncsData(sourceTable);
          const syncParameters = sqlSyncRules.tableSyncsParameters(sourceTable);
          patternResult.tables.push({
            schema,
            name: collection.name,
            replication_id: ['_id'],
            data_queries: syncData,
            parameter_queries: syncParameters,
            errors: errors
          });
        }
      } else {
        const sourceTable = new SourceTable({
          id: 0,
          connectionTag: this.connectionTag,
          objectId: tablePattern.name,
          schema: schema,
          name: tablePattern.name,
          replicaIdColumns: [],
          snapshotComplete: true
        });

        const syncData = sqlSyncRules.tableSyncsData(sourceTable);
        const syncParameters = sqlSyncRules.tableSyncsParameters(sourceTable);
        const collection = collections[0];

        let errors: service_types.ReplicationError[] = [];
        if (collections.length != 1) {
          errors.push({ level: 'warning', message: `Collection ${schema}.${tablePattern.name} not found` });
        } else if (collection.type == 'view') {
          errors.push({ level: 'warning', message: `Collection ${schema}.${tablePattern.name} is a view` });
        } else if (!collection.options?.changeStreamPreAndPostImages?.enabled) {
          errors.push(...validatePostImages(schema, collection));
        }

        patternResult.table = {
          schema,
          name: tablePattern.name,
          replication_id: ['_id'],
          data_queries: syncData,
          parameter_queries: syncParameters,
          errors
        };
      }
    }
    return result;
  }

  async getReplicationLagBytes(options: api.ReplicationLagOptions): Promise<number | undefined> {
    // There is no fast way to get replication lag in bytes in MongoDB.
    // We can get replication lag in seconds, but need a different API for that.
    return undefined;
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const session = this.client.startSession();
    try {
      await this.db.command({ hello: 1 }, { session });
      const head = session.clusterTime?.clusterTime;
      if (head == null) {
        throw new ServiceAssertionError(`clusterTime not available for write checkpoint`);
      }

      const r = await callback(new MongoLSN({ timestamp: head }).comparable);

      // Trigger a change on the changestream.
      await this.db.collection(CHECKPOINTS_COLLECTION).findOneAndUpdate(
        {
          _id: STANDALONE_CHECKPOINT_ID as any
        },
        {
          $inc: { i: 1 }
        },
        {
          upsert: true,
          returnDocument: 'after',
          session
        }
      );
      const time = session.operationTime!;
      if (time == null) {
        throw new ServiceAssertionError(`operationTime not available for write checkpoint`);
      } else if (time.lt(head)) {
        throw new ServiceAssertionError(`operationTime must be > clusterTime`);
      }

      return r;
    } finally {
      await session.endSession();
    }
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    const sampleSize = 50;

    const databases = await this.db.admin().listDatabases({ nameOnly: true });
    const filteredDatabases = databases.databases.filter((db) => {
      return !['local', 'admin', 'config'].includes(db.name);
    });
    const databaseSchemas = await Promise.all(
      filteredDatabases.map(async (db) => {
        /**
         * Filtering the list of database with `authorizedDatabases: true`
         * does not produce the full list of databases under some circumstances.
         * This catches any potential auth errors.
         */
        let collections: mongo.CollectionInfo[];
        try {
          collections = await this.client.db(db.name).listCollections().toArray();
        } catch (e) {
          if (lib_mongo.isMongoServerError(e) && e.codeName == 'Unauthorized') {
            // Ignore databases we're not authorized to query
            return null;
          }
          throw e;
        }

        let tables: service_types.TableSchema[] = [];
        for (let collection of collections) {
          if ([CHECKPOINTS_COLLECTION].includes(collection.name)) {
            continue;
          }
          if (collection.name.startsWith('system.')) {
            // system.views, system.js, system.profile, system.buckets
            // https://www.mongodb.com/docs/manual/reference/system-collections/
            continue;
          }
          if (collection.type == 'view') {
            continue;
          }
          try {
            const sampleDocuments = await this.db
              .collection(collection.name)
              .aggregate([{ $sample: { size: sampleSize } }])
              .toArray();

            if (sampleDocuments.length > 0) {
              const columns = this.getColumnsFromDocuments(sampleDocuments);

              tables.push({
                name: collection.name,
                // Since documents are sampled in a random order, we need to sort
                // to get a consistent order
                columns: columns.sort((a, b) => a.name.localeCompare(b.name))
              });
            } else {
              tables.push({
                name: collection.name,
                columns: []
              });
            }
          } catch (e) {
            if (lib_mongo.isMongoServerError(e) && e.codeName == 'Unauthorized') {
              // Ignore collections we're not authorized to query
              continue;
            }
            throw e;
          }
        }

        return {
          name: db.name,
          tables: tables
        } satisfies service_types.DatabaseSchema;
      })
    );
    return databaseSchemas.filter((schema) => !!schema);
  }

  private getColumnsFromDocuments(documents: mongo.BSON.Document[]) {
    let columns = new Map<string, { sqliteType: sync_rules.ExpressionType; bsonTypes: Set<string> }>();
    for (const document of documents) {
      const parsed = constructAfterRecord(document);
      for (const key in parsed) {
        const value = parsed[key];
        const type = sync_rules.sqliteTypeOf(value);
        const sqliteType = sync_rules.ExpressionType.fromTypeText(type);
        let entry = columns.get(key);
        if (entry == null) {
          entry = { sqliteType, bsonTypes: new Set() };
          columns.set(key, entry);
        } else {
          entry.sqliteType = entry.sqliteType.or(sqliteType);
        }
        const bsonType = this.getBsonType(document[key]);
        if (bsonType != null) {
          entry.bsonTypes.add(bsonType);
        }
      }
    }
    return [...columns.entries()].map(([key, value]) => {
      const internal_type = value.bsonTypes.size == 0 ? '' : [...value.bsonTypes].join(' | ');
      return {
        name: key,
        type: internal_type,
        sqlite_type: value.sqliteType.typeFlags,
        internal_type,
        pg_type: internal_type
      };
    });
  }

  private getBsonType(data: any): string | null {
    if (data == null) {
      // null or undefined
      return 'Null';
    } else if (typeof data == 'string') {
      return 'String';
    } else if (typeof data == 'number') {
      if (Number.isInteger(data)) {
        return 'Integer';
      } else {
        return 'Double';
      }
    } else if (typeof data == 'bigint') {
      return 'Long';
    } else if (typeof data == 'boolean') {
      return 'Boolean';
    } else if (data instanceof mongo.ObjectId) {
      return 'ObjectId';
    } else if (data instanceof mongo.UUID) {
      return 'UUID';
    } else if (data instanceof Date) {
      return 'Date';
    } else if (data instanceof mongo.Timestamp) {
      return 'Timestamp';
    } else if (data instanceof mongo.Binary) {
      return 'Binary';
    } else if (data instanceof mongo.Long) {
      return 'Long';
    } else if (data instanceof RegExp) {
      return 'RegExp';
    } else if (data instanceof mongo.MinKey) {
      return 'MinKey';
    } else if (data instanceof mongo.MaxKey) {
      return 'MaxKey';
    } else if (data instanceof mongo.Decimal128) {
      return 'Decimal';
    } else if (Array.isArray(data)) {
      return 'Array';
    } else if (data instanceof Uint8Array) {
      return 'Binary';
    } else if (typeof data == 'object') {
      return 'Object';
    } else {
      return null;
    }
  }
}
