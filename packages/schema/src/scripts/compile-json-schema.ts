import { configFile } from '@powersync/service-types';
import fs from 'fs';
import path from 'path';
import * as t from 'ts-codec';
import { fileURLToPath } from 'url';
import { MergedServiceConfig } from '../index.js';
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const schemaDir = path.join(__dirname, '../../json-schema');

fs.mkdirSync(schemaDir, { recursive: true });

const mergedConfigSchema = t.generateJSONSchema(MergedServiceConfig, {
  allowAdditional: true,
  parsers: [configFile.portParser]
});

fs.writeFileSync(path.join(schemaDir, 'powersync-config.json'), JSON.stringify(mergedConfigSchema, null, '\t'));
