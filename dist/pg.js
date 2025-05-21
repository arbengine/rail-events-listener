// workers/rail-events-listener/src/pg.ts
import { Pool } from 'pg';
import { pino } from 'pino'; // Import pino directly
// Initialize a logger specific to this module
const pgLogger = pino({ name: 'pg-wrapper', level: process.env.LOG_LEVEL || 'info' });
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    // Recommended settings from node-postgres docs for serverless/lambda environments
    // and general resilience:
    max: process.env.PG_MAX_POOL_SIZE ? parseInt(process.env.PG_MAX_POOL_SIZE, 10) : 10, // Max connections
    idleTimeoutMillis: process.env.PG_IDLE_TIMEOUT_MS ? parseInt(process.env.PG_IDLE_TIMEOUT_MS, 10) : 30000, // How long a client is allowed to remain idle before being closed
    connectionTimeoutMillis: process.env.PG_CONNECTION_TIMEOUT_MS ? parseInt(process.env.PG_CONNECTION_TIMEOUT_MS, 10) : 2000, // How long to wait for a connection to be established
});
pool.on('error', (err) => {
    pgLogger.error({ err }, 'Unexpected error on idle client in pool');
    // In a real-world scenario, you might want to have more sophisticated error handling here,
    // potentially attempting to gracefully shut down or restart the application if the pool becomes unstable.
});
// Optional: A query function that uses the pool
// This simplifies query execution and ensures connection release
async function query(text, params) {
    const start = Date.now();
    const client = await pool.connect();
    try {
        const res = await client.query(text, params);
        const duration = Date.now() - start;
        pgLogger.trace({ query: text, duration, rows: res.rowCount }, 'Executed query');
        return res;
    }
    finally {
        client.release();
    }
}
// Optional: A function to gracefully close the pool
async function closePool() {
    pgLogger.info('Closing database connection pool...');
    await pool.end();
    pgLogger.info('Database connection pool closed.');
}
// Ensure to export what's needed by other modules (e.g., src/index.ts)
export { pool, query, closePool };
