// rail-events-listener/index.js
import 'dotenv/config';
import pkg from 'pg';
import { Connection } from '@temporalio/client';

const { Client } = pkg;

/* ── 1. env sanity ───────────────────────────────────────────── */
const { DATABASE_URL, TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY } = process.env;
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
async function startListener() {
  const pg = new Client({ connectionString: DATABASE_URL });
  await pg.connect();
  await pg.query(`LISTEN rail_events`);
  console.log('📡 listening on rail_events …');

  pg.on('notification', async (msg) => {
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
}

startListener().catch((e) => {
  console.error('startup error', e);
  process.exit(1);
});
