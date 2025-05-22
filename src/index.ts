// workers/rail-events-listener/src/index.ts / – FULL VERSION w/ two-arg signalWithStart
// -----------------------------------------------------------------------------
import 'dotenv/config'; // Ensure env vars are loaded first
import type { Notification, PoolClient } from 'pg';
import retry from 'p-retry';
import { collectDefaultMetrics, Counter } from 'prom-client';
import { getTemporalClient, closeTemporalClient } from './temporalClient.js';
import type { WorkflowClient } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common';
import { pool, closePool, STATEMENT_TIMEOUT_MS, IDLE_TX_TIMEOUT_MS } from './pg.js';

// -----------------------------------------------------------------------------
// lightweight console-based logger (swap for pino in prod if desired)
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

const listenerErrors = new Counter({
  name: 'busywork_listener_errors_total',
  help: 'Unhandled errors in rail-events-listener',
});

const notificationsProcessed = new Counter({
  name: 'rail_events_listener_notifications_processed_total',
  help: 'Total notifications processed',
});

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
  logger.info(`Successfully connected. Default statement_timeout = ${STATEMENT_TIMEOUT_MS}ms, idle_tx_timeout = ${IDLE_TX_TIMEOUT_MS}ms`);

  await client.query(`SET statement_timeout TO 0; SET idle_in_transaction_session_timeout TO 0; SET client_min_messages TO WARNING;`);
  logger.debug('Session timeouts set to 0 for LISTEN socket');

  client.on('error', (err: Error) => {
    listenerErrors.inc();
    logger.error(err, 'PostgreSQL LISTEN client error — will reconnect');
    try { client.release(err); } catch {}
    activeListenerClient = undefined;
  });

  client.on('notification', (msg: Notification) =>
    handleNotification(msg).catch((err) => {
      logger.error({ err, payload: msg.payload }, 'Error in handleNotification');
      listenerErrors.inc();
    }),
  );

  await client.query(`LISTEN ${CHANNEL}`);
  logger.info(`LISTENING on channel: ${CHANNEL}`);
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
    logger.debug({ task_id: ev.task_id, status }, 'DAG_RUNNER=false — ignoring rail event');
    return;
  }

  const wfId = `rail-event-dag-${ev.task_id}-v1`;
  logger.info({ wfId, node_id: ev.node_id, status }, 'Preparing to signal workflow');

  // Lazy-init Temporal client
  if (!temporalClient) temporalClient = await getTemporalClient();

  await temporalClient.signalWithStart('main', {
    /* ----- start-workflow ----- */
    args: [{ taskId: ev.task_id }],
    workflowId: wfId,
    taskQueue: 'dag-runner',

    /* ----- immediate signal ----- */
    signal: 'nodeDone',
    signalArgs: [ev],

    workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
  });

  notificationsProcessed.inc();
  logger.info({ wfId }, 'Successfully signaled workflow');
}

// -----------------------------------------------------------------------------
async function shutdownGracefully(reason?: string) {
  logger.info(`Graceful shutdown ${reason ? 'due to ' + reason : ''}…`);
  try { await closeTemporalClient(); } catch {}
  try { await closePool(); } catch {}
  if (activeListenerClient) {
    try { activeListenerClient.release(); } catch {}
  }
  logger.info('Shutdown complete.');
}

process.on('SIGINT', () => shutdownGracefully('SIGINT').then(() => process.exit(0)));
process.on('SIGTERM', () => shutdownGracefully('SIGTERM').then(() => process.exit(0)));

// -----------------------------------------------------------------------------
// Main bootstrap with initial retry wrapper
import pRetry from 'p-retry';

(async () => {
  logger.info('🚀 Starting rail-events-listener…');
  try {
    await pRetry(bootListener, { retries: 5, minTimeout: 1_000, factor: 2 });
    logger.info('✅ PostgreSQL listener booted');
    temporalClient = await getTemporalClient();
    logger.info('✅ Temporal client ready');
    logger.info('🎉 Application started successfully and is listening for events.');
  } catch (err) {
    logger.fatal({ err }, '💥 Failed to start listener');
    await shutdownGracefully('startup failure');
    process.exit(1);
  }
})();