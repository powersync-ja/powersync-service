import { SourceTableDocumentV3 } from './models.js';

export interface SourceTableIdentity {
  schema: string;
  name: string;
  objectId: number | string | undefined;
  replicaIdColumns: NonNullable<SourceTableDocumentV3['replica_id_columns']>;
}

export function sameStringArray(left: string[], right: string[]) {
  return left.length == right.length && left.every((value, index) => value == right[index]);
}

export function sameReplicaIdColumns(
  left: SourceTableDocumentV3['replica_id_columns'] | undefined,
  right: NonNullable<SourceTableDocumentV3['replica_id_columns']>
) {
  return (
    left != null &&
    left.length == right.length &&
    left.every(
      (column, index) =>
        column.name == right[index].name && column.type == right[index].type && column.type_oid == right[index].type_oid
    )
  );
}

export function matchingSourceTableIdentity(doc: SourceTableDocumentV3, identity: SourceTableIdentity) {
  return (
    doc.schema_name == identity.schema &&
    doc.table_name == identity.name &&
    (identity.objectId == null || doc.relation_id == identity.objectId) &&
    sameReplicaIdColumns(doc.replica_id_columns, identity.replicaIdColumns)
  );
}

export function overlappingSourceTableFilter(
  connectionId: number,
  identity: Pick<SourceTableIdentity, 'schema' | 'name' | 'objectId'>
): Record<string, unknown> {
  const clauses = [{ schema_name: identity.schema, table_name: identity.name }] as Record<string, unknown>[];
  if (identity.objectId != null) {
    clauses.push({ relation_id: identity.objectId });
  }

  return {
    connection_id: connectionId,
    $or: clauses
  };
}
