import { test } from 'node:test';
import { inspect } from 'node:util';
import * as pgwire from '../../src/pgwire/index.js';
import { setup } from './test.js';

// TODO apply inspect options
globalThis.Deno = { inspect };

setup({ ...pgwire, test });
