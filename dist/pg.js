// Root // pg.ts -- shared Postgres helper (adapted from v1.2 example)
// -----------------------------------------------------------------------------
// • Singleton Pool with ENV-driven caps
// • Per-session safety guards (literal SETs -- no bind placeholders)
// -----------------------------------------------------------------------------
import dns from 'dns';
dns.setDefaultResultOrder('ipv4first');
import 'dotenv/config';
import { Pool } from 'pg';
/* ─────────────── ENV-driven tunables (with fallbacks) ─────────────── */
const n = (raw, def) => {
    const val = +(raw || '');
    return Number.isFinite(val) ? val : def;
};
const MAX_CONN = n(process.env.PG_POOL_MAX, 10);
const IDLE_TIMEOUT_MS = n(process.env.PG_POOL_IDLE_MS, 10_000);
const CONN_TIMEOUT_MS = n(process.env.PG_POOL_CONN_TIMEOUT_MS, 2_000);
const STATEMENT_TIMEOUT_MS = n(process.env.PG_STATEMENT_TIMEOUT_MS, 10_000); // Renamed from STATEMENT_TIMEOUT for clarity
const IDLE_TX_TIMEOUT_MS = n(process.env.PG_IDLE_TX_TIMEOUT_MS, 30_000); // Renamed from IDLE_TX_TIMEOUT for clarity
/* ───────────── create a singleton pool (reused everywhere) ────────── */
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    // Allow self-signed SSL certificates. The pg library will use SSL if the
    // server requires it or if the connectionString includes ssl=true or similar flags.
    // This setting is crucial for environments using self-signed certs (e.g., development, some cloud providers).
    ssl: { rejectUnauthorized: false },
    max: MAX_CONN,
    idleTimeoutMillis: IDLE_TIMEOUT_MS,
    connectionTimeoutMillis: CONN_TIMEOUT_MS,
});
/* ─────────── per-session safety settings (runs on connect) ─────────── */
pool.on('connect', async (client) => {
    try {
        // Parameter placeholders are not allowed in SET commands; embed literals directly.
        await client.query(`SET statement_timeout TO ${STATEMENT_TIMEOUT_MS}`);
        await client.query(`SET idle_in_transaction_session_timeout TO ${IDLE_TX_TIMEOUT_MS}`);
    }
    catch (err) {
        console.error('⚠️ failed to apply session settings to PG client', err);
        // Optionally, you might want to release the client if critical settings fail
        // client.release(err); // This would prevent this client from being used
    }
});
pool.on('error', (err) => {
    console.error('⚠️ Unexpected error on idle client in PG pool', err);
});
/* Optional helper: graceful shutdown (for serverless exit hooks) */
async function closePool() {
    try {
        await pool.end();
        console.log('PostgreSQL pool has been closed.');
    }
    catch (err) {
        console.error('⚠️ Error closing PostgreSQL pool', err);
        // throw err; // Re-throw if you want to signal a failed shutdown
    }
}
// Type-safe query function
async function query(text, params) {
    const client = await pool.connect();
    try {
        return await client.query(text, params);
    }
    finally {
        client.release();
    }
}
export { query, pool, // Exporting the pool directly if other parts of the app need to get a client manually
closePool, 
// Exporting timeouts for reference or use elsewhere if needed
STATEMENT_TIMEOUT_MS, IDLE_TX_TIMEOUT_MS };
