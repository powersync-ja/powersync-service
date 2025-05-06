import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { PowerSyncConfigSchema } from '../dist/config/PowerSyncConfig.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(__dirname, '../schema');

fs.mkdirSync(schemaDir, { recursive: true });

fs.writeFileSync(path.join(schemaDir, 'config.json'), JSON.stringify(PowerSyncConfigSchema, null, '\t'));
