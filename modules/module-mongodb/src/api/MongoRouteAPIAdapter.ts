import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { api, ParseSyncConfigOptions, ReplicationHeadCallback } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';

import { logger } from '@powersync/lib-services-framework';
import { CheckpointImplementation } from '../replication/checkpoints/CheckpointImplementation.js';
import { createCheckpointImplementation } from '../replication/checkpoints/create-checkpoint-implementation.js';
import { MongoManager } from '../replication/MongoManager.js';
import { CHECKPOINTS_COLLECTION, detectDocumentDb } from '../replication/replication-utils.js';
import * as types from '../types/types.js';
import { escapeRegExp } from '../utils.js';
import { inferCollectionSchema } from './infer-collection-schema.js';

export class MongoRouteAPIAdapter implements api.RouteAPI {
  protected client: mongo.MongoClient;
  public db: mongo.Db;

  connectionTag: string;
  defaultSchema: string;

  private checkpointImplementation: CheckpointImplementation | null = null;

  constructor(protected config: types.ResolvedConnectionConfig) {
    const manager = new MongoManager(config);
    this.client = manager.client;
    this.db = manager.db;
    this.defaultSchema = manager.db.databaseName;
    this.connectionTag = config.tag ?? sync_rules.DEFAULT_TAG;
  }

  getParseSyncRulesOptions(): ParseSyncConfigOptions {
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
    sqlSyncRules: sync_rules.SyncConfig
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
          const ref: sync_rules.SourceTableRef = {
            connectionTag: this.connectionTag,
            schema,
            name: collection.name
          };
          let errors: service_types.ReplicationError[] = [];
          if (collection.type == 'view') {
            errors.push({ level: 'warning', message: `Collection ${schema}.${tablePattern.name} is a view` });
          } else {
            errors.push(...validatePostImages(schema, collection));
          }
          const syncData = sqlSyncRules.tableSyncsData(ref);
          const syncParameters = sqlSyncRules.tableSyncsParameters(ref);
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
        const ref: sync_rules.SourceTableRef = {
          connectionTag: this.connectionTag,
          schema,
          name: tablePattern.name
        };

        const syncData = sqlSyncRules.tableSyncsData(ref);
        const syncParameters = sqlSyncRules.tableSyncsParameters(ref);
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
    const checkpointImplementation = await this.getCheckpointImplementation();
    return checkpointImplementation.createReplicationHead(callback);
  }

  private async getCheckpointImplementation(): Promise<CheckpointImplementation> {
    if (this.checkpointImplementation == null) {
      const isDocumentDb = await detectDocumentDb(this.db);
      this.checkpointImplementation = createCheckpointImplementation(isDocumentDb, {
        client: this.client,
        db: this.db,
        // The adapter never streams, so it has no real barrier document. This
        // random id is only safe because the adapter only ever calls
        // createReplicationHead, which produces a standalone (stream_id = null)
        // head that every real ChangeStream observes. It must NOT be used for a
        // batch barrier (createBatchCheckpoint stamps this id), or the head would
        // become an own-barrier of a phantom stream that nothing resolves.
        checkpointStreamId: new mongo.ObjectId(),
        logger
      });
    }
    return this.checkpointImplementation;
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    const isDocumentDb = await detectDocumentDb(this.db);
    const databases = await this.db.admin().listDatabases({ nameOnly: true });
    const filteredDatabases = databases.databases.filter((db) => {
      return !['local', 'admin', 'config'].includes(db.name);
    });
    const databaseSchemas: service_types.DatabaseSchema[] = [];
    // Infer one database at a time to avoid accumulating concurrent aggregation results.
    for (const db of filteredDatabases) {
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
          continue;
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
          const columns = await inferCollectionSchema(
            this.client.db(db.name).collection(collection.name),
            isDocumentDb
          );
          tables.push({ name: collection.name, columns });
        } catch (e) {
          if (lib_mongo.isMongoServerError(e) && e.codeName == 'Unauthorized') {
            // Ignore collections we're not authorized to query
            continue;
          }
          throw e;
        }
      }

      databaseSchemas.push({
        name: db.name,
        tables: tables
      });
    }
    return databaseSchemas;
  }
}
