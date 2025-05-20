// ---------------------------------------------------------------------------
// Railway + Supabase: accept self-signed CA for pg TLS
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
// ---------------------------------------------------------------------------

// rail-events-listener/index.js

import 'dotenv/config';
import { Pool } from 'pg';
import { Connection, Client as TemporalClient } from '@temporalio/client';

/* ── 1. env sanity ───────────────────────────────────────────── */
const { DATABASE_URL, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, PG_DIRECT_URL } = process.env;
if (!DATABASE_URL || !TEMPORAL_ADDRESS || !TEMPORAL_NAMESPACE || !TEMPORAL_API_KEY) {
  console.error('❌ Missing env vars. Need DATABASE_URL, TEMPORAL_*');
  process.exit(1);
}

/* ── 2. Temporal connection (one per process) ───────────────── */
let temporalConn;
async function getTemporal() {
  if (temporalConn) return temporalConn;
  temporalConn = await Connection.connect({
    address : TEMPORAL_ADDRESS,
    tls     : {},               // Temporal Cloud → TLS on
    apiKey  : TEMPORAL_API_KEY,
  });
  return temporalConn;
}

/* ── 3. Postgres LISTEN loop ────────────────────────────────── */
async function main() {
  console.log('⏳ Starting rail-events-listener');
  const temporal = await getTemporal();
  const pool = new Pool({
    connectionString: PG_DIRECT_URL || DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });

  let db;
  try {
    db = await pool.connect();
    console.log('✅ Connected to PostgreSQL via Pool');

    await db.query(`LISTEN rail_events`);
    console.log('📡 listening on rail_events …');

    db.on('notification', async (msg) => {
      try {
        const evt = JSON.parse(msg.payload);          // rail_router already emits JSON
        if (!['DONE', 'FAILED'].includes(evt.new_state)) return; // ignore other traffic

        // feature-flag: skip forwarding when USE_DAG_RUNNER != 'true'
        if (process.env.USE_DAG_RUNNER !== 'true') return;

        const conn = await getTemporal();
        const wf   = conn.workflowService;
        const wfId = `dag-${evt.task_id}`;            // convention: one workflow per task

        await wf.signalWithStart({
          workflowId : wfId,
          taskQueue  : 'dag-runner',
          signalName : 'taskEvent',
          signalArgs : [evt],
          workflowType: 'dagRunner',
          input      : [{ taskId: evt.task_id }],
        });
        console.log('➡️  forwarded', evt.node_id, evt.new_state);
      } catch (err) {
        console.error('🚨 listener error:', err);
      }
    });
  } catch (err) {
    console.error('❌ Error connecting to PostgreSQL or setting up listener:', err);
    if (db) {
      db.release(); // Release client back to pool
    }
    process.exit(1);
  }

  console.log('ℹ️ Listener active. Press Ctrl+C to exit.');

  process.on('SIGINT', async () => {
    console.log('\nSIGINT received, shutting down...');
    if (temporalConn) {
      await temporalConn.close();
      console.log('Temporal connection closed.');
    }
    if (db) {
      await pool.end();
      console.log('PostgreSQL pool closed.');
    }
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('❌ Unhandled error in main:', err);
  process.exit(1);
});
