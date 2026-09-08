import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

type Shape =
  | ['null']
  | ['string', number]
  | ['number', number, boolean]
  | ['boolean']
  | ['array', number[]]
  | ['object', Record<string, number>];
const fixture = JSON.parse(
  readFileSync(new URL('../../src/fixtures/sample-op-shapes.json', import.meta.url), 'utf8')
) as {
  nodes: Shape[];
  profiles: { outputBytes: number; shape: number }[];
};
export const sampleOutputSizes = fixture.profiles.map((profile) => profile.outputBytes);

function generate(shape: Shape, seed: string): unknown {
  switch (shape[0]) {
    case 'null':
      return null;
    case 'boolean':
      return parseInt(createHash('sha256').update(seed).digest('hex').slice(0, 2), 16) % 2 === 0;
    case 'number': {
      const fraction = parseInt(createHash('sha256').update(seed).digest('hex').slice(0, 6), 16) / 0x1000000;
      const value = (1 + fraction * 8) * 10 ** Math.min(shape[1] - 1, 14);
      return shape[2] ? Math.floor(value) : Math.round(value * 1000) / 1000;
    }
    case 'string': {
      let value = '';
      for (let chunk = 0; value.length < shape[1]; chunk++)
        value += createHash('sha256').update(`${seed}/${chunk}`).digest('base64url');
      return value.slice(0, shape[1]);
    }
    case 'array':
      return shape[1].map((child, index) => generate(fixture.nodes[child], `${seed}/${index}`));
    case 'object':
      return Object.fromEntries(
        Object.entries(shape[1]).map(([name, child]) => [name, generate(fixture.nodes[child], `${seed}/${name}`)])
      );
  }
}

/** Source documents derived solely from anonymized type/size profiles of processed ops. */
export function sampleOpDocument(index: number, revision: number): Record<string, unknown> {
  const profile = fixture.profiles[index % fixture.profiles.length];
  return generate(fixture.nodes[profile.shape], `${index}/${revision}`) as Record<string, unknown>;
}
