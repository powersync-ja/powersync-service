import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const require = createRequire(import.meta.url);
const dirname = path.dirname(fileURLToPath(import.meta.url));
let native = null;

function getNative() {
  if (native != null) {
    return native;
  }

  native = require(path.join(dirname, 'mongo-after-record-rs.node'));
  return native;
}

export class MongoAfterRecordConverter {
  #inner;

  constructor() {
    this.#inner = new (getNative().NativeMongoAfterRecordConverter)();
  }

  constructAfterRecordSerialized(bsonBytes) {
    return this.#inner.constructAfterRecordJson(bsonBytes);
  }

  constructAfterRecordObject(bsonBytes) {
    return this.#inner.constructAfterRecordObject(bsonBytes);
  }

  constructAfterRecord(bsonBytes) {
    return this.constructAfterRecordObject(bsonBytes);
  }
}
