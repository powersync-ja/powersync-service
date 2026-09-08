import { deserialize } from 'bson';
import { readFile, writeFile } from 'node:fs/promises';

// Only type/size descriptors and synthetic field names are exported. No source values,
// field names, IDs, bucket names or hashes of source values are retained.
const [input, output] = process.argv.slice(2);
if (!input || !output) throw new Error('Usage: import-op-shapes.ts INPUT.bson OUTPUT.json');
const buffer = await readFile(input);
const records: Record<string, unknown>[] = [];
for (let offset = 0; offset < buffer.length; ) {
  const size = buffer.readInt32LE(offset);
  if (size < 5 || offset + size > buffer.length) throw new Error('Invalid BSON document length');
  const document = deserialize(buffer.subarray(offset, offset + size));
  const wrapped = Object.values(document).find(Array.isArray);
  records.push(...(wrapped ?? [document]));
  offset += size;
}
const fieldNames = new Map<string, string>();
const nodes: unknown[] = [];
const nodeIds = new Map<string, number>();
function shape(value: unknown): number {
  const descriptor = describe(value);
  const key = JSON.stringify(descriptor);
  let id = nodeIds.get(key);
  if (id == null) {
    id = nodes.length;
    nodes.push(descriptor);
    nodeIds.set(key, id);
  }
  return id;
}
function describe(value: unknown): unknown {
  if (value === null) return ['null'];
  if (Array.isArray(value)) return ['array', value.map(shape)];
  switch (typeof value) {
    case 'string':
      return ['string', Buffer.byteLength(value)];
    case 'boolean':
      return ['boolean'];
    case 'number':
      return ['number', Math.max(1, Math.floor(Math.log10(Math.max(1, Math.abs(value)))) + 1), Number.isInteger(value)];
    case 'object':
      return [
        'object',
        Object.fromEntries(
          Object.entries(value!).map(([key, child]) => {
            let name = fieldNames.get(key);
            if (!name) {
              name = `field_${String(fieldNames.size + 1).padStart(3, '0')}`;
              fieldNames.set(key, name);
            }
            return [name, shape(child)];
          })
        )
      ];
    default:
      throw new Error(`Unsupported payload type: ${typeof value}`);
  }
}
const profiles = records.flatMap((record) => {
  const payloads = Object.values(record).flatMap((value) => {
    if (typeof value !== 'string') return [];
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
        ? [{ parsed, bytes: Buffer.byteLength(value) }]
        : [];
    } catch {
      return [];
    }
  });
  const payload = payloads.sort((a, b) => b.bytes - a.bytes)[0];
  if (!payload) return [];
  // Bucket operation payloads encode nested source arrays/objects as JSON strings.
  // Reconstruct those source values so SELECT produces similarly encoded output.
  for (const [key, value] of Object.entries(payload.parsed)) {
    if (typeof value !== 'string') continue;
    try {
      const decoded = JSON.parse(value);
      if (decoded !== null && typeof decoded === 'object') payload.parsed[key] = decoded;
    } catch {
      /* Ordinary strings remain strings. */
    }
  }
  return [{ outputBytes: payload.bytes, shape: shape(payload.parsed) }];
});
if (!profiles.length) throw new Error('No JSON object payloads found in BSON operations');
await writeFile(output, JSON.stringify({ version: 1, nodes, profiles }) + '\n');
console.log(`Exported ${profiles.length} anonymized shape profiles (${fieldNames.size} renamed fields)`);
