import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

export const DEFAULT_CERTS = await loadDefaultCertificates();

async function loadDefaultCertificates() {
  const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), '../ca');
  const files = await fs.readdir(dir);
  let sum = '';
  for (let file of files) {
    if (file.endsWith('.pem')) {
      sum += (await fs.readFile(path.resolve(dir, file), 'utf-8')) + '\n';
    }
  }
  return sum;
}
