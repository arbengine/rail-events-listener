// src/pg.js
import pg from 'pg'; // pg is a CJS module, default import is { Pool, Client etc. }
const { Pool } = pg; // Destructure Pool from the default import
import pino from 'pino';
const pgLogger = pino({ name: 'pg-wrapper-js', level: process.env.LOG_LEVEL || 'info' });
// Default values for new timeout settings
const PG_STATEMENT_TIMEOUT_MS_DEFAULT = 60000; // 60s
const PG_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS_DEFAULT = 30000; // 30s
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: process.env.PG_MAX_POOL_SIZE ? parseInt(process.env.PG_MAX_POOL_SIZE, 10) : 10,
    idleTimeoutMillis: process.env.PG_IDLE_TIMEOUT_MS ? parseInt(process.env.PG_IDLE_TIMEOUT_MS, 10) : 30000,
    connectionTimeoutMillis: process.env.PG_CONNECTION_TIMEOUT_MS ? parseInt(process.env.PG_CONNECTION_TIMEOUT_MS, 10) : 2000,
    // Add stricter timeout configurations
    statement_timeout: process.env.PG_STATEMENT_TIMEOUT_MS
        ? parseInt(process.env.PG_STATEMENT_TIMEOUT_MS, 10)
        : PG_STATEMENT_TIMEOUT_MS_DEFAULT,
    idle_in_transaction_session_timeout: process.env.PG_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS
        ? parseInt(process.env.PG_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS, 10)
        : PG_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS_DEFAULT,
});
pool.on('error', (err) => {
    // Replaced console.error with pgLogger.error for consistent logging
    pgLogger.error({ err, message: err.message }, 'Unexpected error on idle client in pool (pg.js)');
});
async function query(text, params) {
    const start = Date.now();
    const client = await pool.connect();
    try {
        const res = await client.query(text, params);
        const duration = Date.now() - start;
        pgLogger.trace({ query: text, duration, rows: res.rowCount }, 'Executed query (pg.js)');
        return res;
    }
    finally {
        client.release();
    }
}
async function closePool() {
    pgLogger.info('Closing database connection pool (pg.js)...');
    await pool.end();
    pgLogger.info('Database connection pool closed (pg.js).');
}
export { pool, query, closePool };
