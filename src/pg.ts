// workers/rail-events-listener/src/pg.ts
// re-export the CJS helper as ESM-friendly symbols
// ------------------------------------------------
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const sharedPg = require('../../pg.js');     // ← adjust relative path if needed

export const pool       = sharedPg.pool;
export const query      = sharedPg.query;
export const closePool  = sharedPg.closePool;
