import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { syncRulesSchema } from '../dist/json_schema.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(__dirname, '../schema');

fs.mkdirSync(schemaDir, { recursive: true });

fs.writeFileSync(path.join(schemaDir, 'sync_rules.json'), JSON.stringify(syncRulesSchema, null, '\t'));
