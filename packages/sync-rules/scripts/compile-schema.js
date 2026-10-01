import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { syncRulesSchema } from '../dist/json_schema.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(__dirname, '../schema');

fs.mkdirSync(schemaDir, { recursive: true });

const contents = JSON.stringify(syncRulesSchema, null, '\t');
fs.writeFileSync(path.join(schemaDir, 'sync_config.json'), contents);

// For backwards compatibility with the CLI embedding an unpkg url pointing to this file in a
// `yaml-language-server: $schema=` comment.
fs.writeFileSync(path.join(schemaDir, 'sync_rules.json'), contents);
