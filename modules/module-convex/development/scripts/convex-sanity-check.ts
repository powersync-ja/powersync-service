import { ConvexHttpClient } from 'convex/browser';
import * as dotenv from 'dotenv';
import { api } from '../../convex/_generated/api.js';

dotenv.config({ path: '.env.local' });

const client = new ConvexHttpClient(process.env['CONVEX_URL']!);

// List all lists
const lists = await client.query(api.lists.get);
console.log('Fetched the following lists from Convex');
console.log(JSON.stringify(lists, null, '\t'));
