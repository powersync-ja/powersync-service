import * as fs from 'fs/promises';
import { resolve } from 'path';

export const DEFAULT_CERTS = await loadDefaultCertificates();

async function loadDefaultCertificates() {
  const dir = new URL('../ca', import.meta.url).pathname;
  const files = await fs.readdir(dir);
  let sum = '';
  for (let file of files) {
    if (file.endsWith('.pem')) {
      sum += (await fs.readFile(resolve(dir, file), 'utf-8')) + '\n';
    }
  }
  return sum;
}
