import { mongo } from '@powersync/lib-service-mongodb';
import { applyRowContext, CompatibilityContext, SqliteRow } from '@powersync/service-sync-rules';
import { constructAfterRecord } from './MongoRelation.js';

export interface SourceRowConverter {
  /**
   * Convert a Buffer, containing BSON document bytes, to a SqliteRow for use
   * in sync config.
   *
   * The returned replicaId is the plain parsed bson _id value, to preserve the source id as-is.
   */
  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any };

  /**
   * Convert a document, as received from MongoDB query or change stream, to
   * a SqliteRow for use in sync config.
   *
   * The document must be parsed using { useBigInt64: true }.
   */
  documentToSqliteRow(source: mongo.Document): SqliteRow;
}

export class DefaultSourceRowConverter implements SourceRowConverter {
  constructor(private readonly compatibilityContext: CompatibilityContext) {}

  rawToSqliteRow(source: Buffer): { row: SqliteRow; replicaId: any } {
    const parsed = mongo.BSON.deserialize(source, { useBigInt64: true });
    const row = this.documentToSqliteRow(parsed);
    const replicaId = parsed._id;
    return { row, replicaId };
  }

  documentToSqliteRow(document: mongo.Document): SqliteRow {
    const input = constructAfterRecord(document);
    return applyRowContext<never>(input, this.compatibilityContext);
  }
}
