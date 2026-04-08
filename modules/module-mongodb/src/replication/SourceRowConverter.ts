import { mongo } from '@powersync/lib-service-mongodb';
import { applyRowContext, CompatibilityContext, SqliteRow } from '@powersync/service-sync-rules';
import { constructAfterRecord } from './MongoRelation.js';

export interface SourceRowConverter {
  /**
   * Convert a Buffer, containing BSON document bytes, to a SqliteRow for use
   * in sync config.
   */
  rawToSqliteRow(source: Buffer): SqliteRow;

  /**
   * Convert a document, as received from MongoDB query or change stream, to
   * a SqliteRow for use in sync config.
   *
   * The document must be parsed using { useBigInt64: true }.
   */
  documentToSqliteRow(source: mongo.Document): SqliteRow;
}

export class DefaultSourceRowConverter implements SourceRowConverter {
  constructor(public readonly compatibilityContext: CompatibilityContext) {}

  rawToSqliteRow(source: Buffer): SqliteRow {
    const parsed = mongo.BSON.deserialize(source, { useBigInt64: true });
    return this.documentToSqliteRow(parsed);
  }

  documentToSqliteRow(document: mongo.Document): SqliteRow {
    const input = constructAfterRecord(document);
    return applyRowContext<never>(input, this.compatibilityContext);
  }
}
