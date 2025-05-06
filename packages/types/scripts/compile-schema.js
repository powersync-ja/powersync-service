import fs from 'fs';
import path from 'path';
import * as t from 'ts-codec';
import { fileURLToPath } from 'url';
import { portParser, powerSyncConfig } from '../dist/config/PowerSyncConfig.js';

const schema = t.generateJSONSchema(powerSyncConfig, {
  allowAdditional: true, // This is relevant since the replication config is generic at this point
  parsers: [portParser]
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(__dirname, '../schema');

fs.mkdirSync(schemaDir, { recursive: true });

fs.writeFileSync(path.join(schemaDir, 'config.json'), JSON.stringify(schema, null, '\t'));
