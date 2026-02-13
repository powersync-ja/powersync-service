export type MongoAfterRecordValue = string | number | bigint | Uint8Array | null;

export class MongoAfterRecordConverter {
  constructor();
  constructAfterRecordSerialized(bsonBytes: Uint8Array): string;
  constructAfterRecordObject(bsonBytes: Uint8Array): Record<string, MongoAfterRecordValue>;
  constructAfterRecord(bsonBytes: Uint8Array): Record<string, MongoAfterRecordValue>;
}
