import 'dotenv/config'; // Ensure env vars are loaded first //
import type { Notification, PoolClient } from 'pg';
import retry from 'p-retry';
import { collectDefaultMetrics, Counter } from 'prom-client';
import { getTemporalClient, closeTemporalClient } from './temporalClient.js';
import type { WorkflowClient } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common';
import { pool, closePool, STATEMENT_TIMEOUT_MS, IDLE_TX_TIMEOUT_MS } from './pg.js';

// -----------------------------------------------------------------------------
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

const CHANNEL        = process.env.PG_CHANNEL || 'rail_events';
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
  const heartbeat = setInterval(() => {
    client
      .query('SELECT 1')                // light, uses the same socket
      .then(() => {
        logger.info(                 // ← show in every log view
          logCtx(),
          '❤️ listener heartbeat OK',
        );
      })
      .catch((err: Error) => {
        logger.warn(
          logCtx({ err }),
          '💔 heartbeat failed – connection likely lost',
        );
      });
  }, 30_000);

  /* clear the interval when this client ends */
  client.once('end', () => clearInterval(heartbeat));
}

// -----------------------------------------------------------------------------
/** Parse and forward one NOTIFY payload. */
async function handleNotification(msg: Notification): Promise<void> {
  if (msg.channel !== CHANNEL || !msg.payload) return;

  let ev: any;
  try { ev = JSON.parse(msg.payload); } catch { return; }
  if (typeof ev !== 'object' || ev === null) return;

  const status = ev.event_type ?? ev.state ?? ev.type;
  if (!['DONE', 'FAILED'].includes(status)) return;

  if (!USE_DAG_RUNNER) {
    logger.debug(logCtx({ task_id: ev.task_id, status }), '⏭️ DAG_RUNNER=false — skipping event');
    return;
  }

  const wfId = `rail-event-dag-${ev.task_id}-v1`;
  logger.info(logCtx({ wfId, node_id: ev.node_id, status }), '📤 Preparing to signal Temporal workflow…');

  // Lazy-init Temporal client
  if (!temporalClient) temporalClient = await getTemporalClient();

  await temporalClient.signalWithStart('main', {
    args: [{ taskId: ev.task_id }],
    workflowId: wfId,
    taskQueue: 'dag-runner',
    signal: 'nodeDone',
    signalArgs: [ev],
    workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
  });

  notificationsProcessed.inc({ status, instance_id: INSTANCE_ID });
  logger.info(logCtx({ wfId }), '✅ Workflow signaled successfully');
}

// -----------------------------------------------------------------------------
async function shutdownGracefully(reason?: string) {
  logger.info(logCtx({ reason }), '🛑 Graceful shutdown requested');

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
    logger.info(logCtx(), '🎉 Application started successfully and is listening for events.');
  } catch (err) {
    logger.fatal(logCtx({ err }), '💥 Failed to start listener');
    await shutdownGracefully('startup failure');
    process.exit(1);
  }
})();
