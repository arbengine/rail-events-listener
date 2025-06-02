import 'dotenv/config'; // Ensure env vars are loaded first //
import type { Notification, PoolClient } from 'pg';
import retry from 'p-retry';
import { collectDefaultMetrics, Counter } from 'prom-client';
import { getTemporalClient, closeTemporalClient } from './temporalClient.js';
import type { WorkflowClient } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common';
import {
  pool,
  query,
  closePool,
  STATEMENT_TIMEOUT_MS,
  IDLE_TX_TIMEOUT_MS,
} from './pg.js';

/* ─── NEW: NATS client ──────────────────────────────── */
import { connect as natsConnect, StringCodec } from 'nats';

/* one global connection reused by every notify */
const nc = await natsConnect({
  servers: process.env.NATS_URL || 'nats://nats-scalable:4222',
});
const sc = StringCodec();

console.log('[rail-events] ✅ connected to NATS');

import { RailEvent, toDelta, BroadcastDelta } from './utils/delta.js';
import { initializeWebSocketServer, broadcast, closeWebSocketServer } from './websocketServer.js';

// ----------------------------------------------------------------------------
// Lightweight console-based logger (swap for pino in prod if desired)
export const logger = {
  info : (...a: any[]) => console.log(...a),
  warn : (...a: any[]) => console.warn(...a),
  error: (...a: any[]) => console.error(...a),
  debug: (...a: any[]) => console.debug(...a),
  trace: (...a: any[]) => console.trace(...a),
  fatal: (...a: any[]) => console.error(...a),
};

// -----------------------------------------------------------------------------
collectDefaultMetrics({ prefix: 'rail_events_listener_' });

const lastEventByNode = new Map<string, RailEvent>(); // key = taskId:nodeId

const CHANNEL        = process.env.PG_CHANNEL || 'rail_event';
const USE_DAG_RUNNER = process.env.DAG_RUNNER === 'true';
const INSTANCE_ID    = process.env.HOSTNAME || 'unknown';

const listenerErrors = new Counter({
  name: 'busywork_listener_errors_total',
  help: 'Unhandled errors in rail-events-listener',
  labelNames: ['instance_id'],
});

const notificationsProcessed = new Counter({
  name: 'rail_events_listener_notifications_processed_total',
  help: 'Total notifications processed',
  labelNames: ['status', 'instance_id'],
});

const logCtx = (extra: Record<string, any> = {}) => ({ instance_id: INSTANCE_ID, ...extra });

let temporalClient: WorkflowClient | undefined;
let activeListenerClient: PoolClient | undefined; // track for graceful shutdown

// -----------------------------------------------------------------------------
/** Boot a dedicated, long-lived LISTEN socket (retries inside p-retry). */
export async function bootListener(): Promise<void> {
  logger.info('Attempting to connect to PostgreSQL for LISTEN…');

  if (activeListenerClient) {
    try { activeListenerClient.release(); } catch {}
    activeListenerClient = undefined;
  }

  const client = await pool.connect();
  activeListenerClient = client;

  logger.info(
    logCtx({
      statement_timeout: STATEMENT_TIMEOUT_MS,
      idle_tx_timeout: IDLE_TX_TIMEOUT_MS,
    }),
    '✅ Connected to PG for LISTEN',
  );

  await client.query(`SET statement_timeout TO 0; SET idle_in_transaction_session_timeout TO 0; SET client_min_messages TO WARNING;`);
  logger.debug('Session timeouts set to 0 for LISTEN socket');

  client.on('error', (err: Error) => {
    listenerErrors.inc({ instance_id: INSTANCE_ID });
    logger.error(logCtx({ err }), '💥 PostgreSQL LISTEN client error — reconnecting');
    try { client.release(err); } catch {}
    activeListenerClient = undefined;
  });

  client.on('notification', (msg: Notification) =>
    handleNotification(msg).catch((err) => {
      logger.error(logCtx({ err, payload: msg.payload }), '💥 Error in handleNotification');
      listenerErrors.inc({ instance_id: INSTANCE_ID });
    }),
  );

  await client.query(`LISTEN ${CHANNEL}`);
  logger.info(logCtx({ channel: CHANNEL }), '🔔 LISTENING for events');

  /* ---------- 30-second SQL heartbeat ---------- */
  const heartbeat = setInterval(async () => {
    try {
      await query('SELECT 1');   // ✅ grabs a fresh pool connection
      logger.info(logCtx(), '❤️ listener heartbeat OK');
    } catch (err: any) {
      logger.warn(logCtx({ err }), '💔 heartbeat failed – connection likely lost');
    }
  }, 30_000);

  /* clear the interval when this client ends */
  client.once('end', () => clearInterval(heartbeat));
}

// ------------------------------------------------------------------------------
/** Parse and forward one NOTIFY payload. */
async function handleNotification(msg: Notification): Promise<void> {
  if (msg.channel !== CHANNEL || !msg.payload) return;

  /* ── parse JSON ─────────────────────────────── */
  let raw: any;
  try {
    raw = JSON.parse(msg.payload);

    /* 1️⃣  copy camelCase → snake_case */
    if (raw.taskId        && !raw.task_id)       raw.task_id       = raw.taskId;
    if (raw.nodeId        && !raw.node_id)       raw.node_id       = raw.nodeId;
    if (raw.eventSubtype  && !raw.event_subtype) raw.event_subtype = raw.eventSubtype;

    /* 2️⃣  trim *key* names that carry stray spaces */
    for (const k of Object.keys(raw)) {
      const trimmed = k.trim();
      if (trimmed !== k && raw[trimmed] === undefined) {
        raw[trimmed] = raw[k];
        delete raw[k];
      }
    }
  } catch (err) {
    logger.error(logCtx({ err, payload: msg.payload }), 'Failed JSON.parse');
    return;
  }

  /* 3️⃣  shape-check AFTER normalisation */
  if (typeof raw !== 'object' || !raw.task_id || !raw.node_id || !raw.state) {
    logger.warn(logCtx({ payload: msg.payload, normalised: raw }),
                'Received payload does not conform to RailEvent');
    return;
  }

  const curr: RailEvent = raw as RailEvent;

  const key = `${curr.task_id}:${curr.node_id}`;
  const prev = lastEventByNode.get(key) ?? null;
  let snapshotVersion: number = -1; // Default/error value

  try {
    const { rows } = await query('SELECT txid_current() AS v');
    if (rows && rows.length > 0 && rows[0].v !== null && rows[0].v !== undefined) {
      const parsedVersion = Number(rows[0].v);
      if (!isNaN(parsedVersion)) {
        snapshotVersion = parsedVersion;
        logger.info(logCtx({ taskId: curr.task_id, nodeId: curr.node_id, snapshotVersion }), 'Retrieved snapshotVersion for delta');
      } else {
        logger.warn(logCtx({ taskId: curr.task_id, nodeId: curr.node_id, rawValue: rows[0].v }), 'Failed to convert txid_current to Number for snapshotVersion');
      }
    } else {
      logger.warn(logCtx({ taskId: curr.task_id, nodeId: curr.node_id, rows }), 'Failed to retrieve txid_current or rows were empty/undefined for snapshotVersion');
    }
  } catch (err: any) {
    logger.error(logCtx({ err: { message: err.message, stack: err.stack }, taskId: curr.task_id, nodeId: curr.node_id }), '💥 Error fetching txid_current for snapshot version');
    // snapshotVersion remains -1, indicating an issue. The frontend should handle this gracefully.
  }

  const delta: BroadcastDelta = toDelta(curr!, prev, snapshotVersion);

  /* ── NEW: publish READY over NATS ─────────────────────────────── */
  if (delta.state === 'WAITING_AI' && delta.eventSubtype === 'READY') {
    // grab scratch-pad once for the prompt
    const { rows: [node] } = await query(
      `SELECT pending_instructions_md AS md
         FROM execution_nodes
        WHERE node_id = $1`,
      [delta.nodeId],
    );

    const payload = {
      taskId : delta.taskId,
      nodeId : delta.nodeId,
      attempt: 1,                // bump on every retry later
      md     : node?.md ?? null, // scratch-pad or null
    };

    const subj = `busywork.node.ready.${delta.nodeId}`;
    nc.publish(subj, sc.encode(JSON.stringify(payload)));
    logger.debug(logCtx({ node: delta.nodeId }), '📤 NATS busywork.node.ready published');
  }
  /* ─────────────────────────────────────────────────────────────── */
  broadcast(delta); // Call the imported broadcast function

/* ── NEW: publish “node done” over NATS for side-cars ── */
if (curr.state === 'DONE') {
  try {
    await nc.publish(
      `busywork.node.done.${curr.node_id}`,      // subject
      sc.encode(JSON.stringify({
        taskId: curr.task_id,
        nodeId: curr.node_id,
      })),
    );
    logger.debug(logCtx({ node: curr.node_id }), '📤 NATS busywork.node.done published');
  } catch (err) {
    logger.error(logCtx({ err }), '💥 NATS publish failed (non-fatal)');
  }
}
  logger.info({ deltaPayload: true, delta, taskId: curr.task_id, nodeId: curr.node_id }); // Log delta

  lastEventByNode.set(key, curr); // Update last event for this node

  const statusForTemporal = curr.state; // Assuming 'state' is the primary status for Temporal
  if (!['DONE', 'FAILED'].includes(statusForTemporal)) {
    logger.debug(logCtx({ task_id: curr.task_id, status: statusForTemporal }), 'Skipping Temporal signal for non-terminal status based on original logic');
    return;
  }

  if (!USE_DAG_RUNNER) {
    logger.debug(logCtx({ task_id: curr.task_id, status: statusForTemporal }), '⏭️ DAG_RUNNER=false — skipping event');
    return;
  }

  const wfId = `rail-event-dag-${curr.task_id}-v1`;
  logger.info(logCtx({ wfId, node_id: curr.node_id, status: statusForTemporal }), '📤 Preparing to signal Temporal workflow…');

  // Lazy-init Temporal client
  if (!temporalClient) temporalClient = await getTemporalClient();

  await temporalClient.signalWithStart('main', {
    args: [{ taskId: curr.task_id }],
    workflowId: wfId,
    taskQueue: 'dag-runner',
    signal: 'nodeDone',
    signalArgs: [curr],
    workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
  });

  notificationsProcessed.inc({ status: statusForTemporal, instance_id: INSTANCE_ID });
  logger.info(logCtx({ wfId }), '✅ Workflow signaled successfully');
}

// -----------------------------------------------------------------------------
async function shutdownGracefully(reason?: string) {
  logger.info(logCtx({ reason }), '🛑 Graceful shutdown requested');

  await new Promise<void>(resolve => closeWebSocketServer(resolve)); // Close WebSocket server
  try { await closeTemporalClient(); } catch {} 
  try { await closePool(); } catch {} 
  if (activeListenerClient) {
    try { activeListenerClient.release(); } catch {}
  }

  logger.info(logCtx({
    metrics: {
      processed: notificationsProcessed.get(),
      errors: listenerErrors.get(),
    }
  }), '📊 Shutdown complete with final metrics');
}

process.on('SIGINT', () => shutdownGracefully('SIGINT').then(() => process.exit(0)));
process.on('SIGTERM', () => shutdownGracefully('SIGTERM').then(() => process.exit(0)));

// -----------------------------------------------------------------------------
import pRetry from 'p-retry';

(async () => {
  logger.info(logCtx(), '🚀 Starting rail-events-listener bootstrap…');
  logger.info(logCtx({ USE_DAG_RUNNER }), `🚦 DAG_RUNNER = ${USE_DAG_RUNNER}`);
  logger.info(logCtx({ channel: CHANNEL }), `📡 Subscribing to PG channel: ${CHANNEL}`);

  try {
    await pRetry(bootListener, { retries: 5, minTimeout: 1_000, factor: 2 });
    logger.info(logCtx(), '✅ PostgreSQL listener booted');
    temporalClient = await getTemporalClient();
    logger.info(logCtx(), '✅ Temporal client ready');
    initializeWebSocketServer(logger); // Initialize WebSocket server with logger
    logger.info(logCtx(), '🎉 Application started successfully and is listening for events.');
  } catch (err) {
    logger.fatal(logCtx({ err }), '💥 Failed to start listener');
    await shutdownGracefully('startup failure');
    process.exit(1);
  }
})();
