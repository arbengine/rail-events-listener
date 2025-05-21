import type { Notification, PoolClient } from 'pg';
import retry from 'p-retry';
import {
  collectDefaultMetrics,
  Counter,
} from 'prom-client';
import pino from 'pino';
import { getTemporalClient } from './temporalClient.js';
import type { WorkflowClient } from '@temporalio/client';
import { pool } from './pg.js';

const pinoInstance = pino as any; // Cast to any to bypass type checking for this line
export const logger = pinoInstance({ level: process.env.LOG_LEVEL || 'info' });

collectDefaultMetrics({ prefix: 'rail_events_listener_' });

// Commenting out Pushgateway section due to 'prom' not found and to simplify.
// It can be re-enabled later if PUSHGATEWAY_URL is configured and prom-client imports are adjusted.
// const CHANNEL = 'rail_events'; // Defined below
// const USE_DAG_RUNNER = process.env.DAG_RUNNER_ACTIVE === 'true'; // Defined below

// const pgw = new prom.Pushgateway(process.env.PUSHGATEWAY_URL!); // prom is not defined
// setInterval(
//   () => {
//     if (pgw) { // Check if pgw is initialized
//        pgw.pushAdd({ jobName: 'rail-events-listener' })
//         .catch(err => logger.error({ err }, 'Error pushing metrics to Pushgateway'));
//     }
//   },
//   30_000,
// );

const CHANNEL = process.env.PG_CHANNEL || 'rail_events';
const USE_DAG_RUNNER = process.env.DAG_RUNNER === 'true';

const listenerErrors = new Counter({
  name: 'busywork_listener_errors_total',
  help: 'Unhandled errors in rail-events-listener',
});

const notificationsProcessed = new Counter({
  name: 'rail_events_listener_notifications_processed_total',
  help: 'Total notifications processed by the rail events listener',
});

let temporalClient: WorkflowClient | undefined;

/** Boot a dedicated, long-lived LISTEN socket. */
async function bootListener(): Promise<void> {
  logger.info('Attempting to connect to PostgreSQL for LISTEN...');
  let listenerClient: PoolClient | undefined;
  try {
    const client = await pool.connect(); 
    logger.info('Successfully connected client from shared pool for LISTEN.');
    listenerClient = client; // Store the successfully connected client

    // Crucially, disable server-side timeouts on THIS SPECIFIC socket from the shared pool
    logger.debug('Setting session timeouts for LISTEN client...');
    await client.query(`
      SET statement_timeout TO 0;
      SET idle_in_transaction_session_timeout TO 0;
      SET client_min_messages TO WARNING; -- Reduce verbosity of some logs
    `);
    logger.debug('Session timeouts set.');

    client.on('error', (err: Error) => {
      listenerErrors.inc();
      logger.error(err, 'PostgreSQL client error on LISTEN connection (will attempt to reconnect if connection ends)');
      // The 'pg' library typically handles client removal from pool on 'error' or 'end'.
      // If an error occurs that doesn't end the connection but makes it unusable,
      // explicit client.release(err) might be needed, but rely on p-retry for now.
    });

    client.on('notification', (msg: Notification) =>
      handleNotification(msg).catch(err => {
        logger.error({ err, msgPayload: msg.payload }, 'Error in handleNotification');
        listenerErrors.inc(); // Ensure errors in notification handling are counted
      }),
    );

    await client.query(`LISTEN ${CHANNEL}`);
    logger.info(`LISTENING on PostgreSQL channel: ${CHANNEL}`);

    /** Reconnect logic if connection ends */
    client.on('end', () => {
      logger.warn('PostgreSQL client LISTEN connection ended. Initiating reconnection sequence.');
      // client.release(); // Client is auto-removed from pool on 'end' if it was in use.
      
      retry(bootListener, {
        retries: process.env.LISTENER_RECONNECT_RETRIES ? parseInt(process.env.LISTENER_RECONNECT_RETRIES, 10) : 10, // More retries for a listener
        minTimeout: 1_000,
        maxTimeout: 45_000, // Slightly longer max timeout
        factor: 2.5,        // Steeper backoff
        onFailedAttempt: error => {
          logger.warn(
            { attemptNumber: error.attemptNumber, retriesLeft: error.retriesLeft, error: error.message },
            'Listener reconnect attempt failed. Retrying...'
          );
        },
      }).catch(finalError => {
        logger.fatal(finalError, 'All listener reconnection attempts failed. The rail-events-listener will now exit.');
        process.exit(1); // Exit if all retries are exhausted
      });
    });
    // No explicit return here, the function's purpose is to set up the listener
  } catch (err: any) { // Use 'any' to access potential pg-specific error properties
    // 🔥 give Railway something it can't collapse
    console.error('🐞 PG connection/SET/LISTEN failed →', JSON.stringify(err, null, 2));
    
    logger.error(
      {
        message: err?.message,
        stack: err?.stack,
        code: err?.code, // e.g., ECONNREFUSED, ENOTFOUND (from pg)
        errno: err?.errno,
        syscall: err?.syscall,
        address: err?.address,
        port: err?.port,
        fullError: err // Log the full error object for detailed inspection
      },
      'Failed to connect or setup LISTEN client'
    );
    if (listenerClient) {
      // Release the client back to the pool, marking it as errored so it's discarded
      listenerClient.release(true); 
    }
    throw err; // Rethrow to be handled by p-retry
  }
}

/** Process NOTIFY payloads one by one */
async function handleNotification(msg: Notification): Promise<void> {
  if (msg.channel !== CHANNEL) {
    logger.warn({ actual: msg.channel, expected: CHANNEL }, 'Notification received on unexpected channel');
    return;
  }

  let ev;
  try {
    if (msg.payload == null || msg.payload.trim() === '') {
      logger.warn('Received empty notification payload - skipping');
      return;
    }
    ev = JSON.parse(msg.payload);
  } catch (parseError) {
    logger.warn({ payload: msg.payload, error: parseError }, 'Malformed JSON notification payload - skipping');
    listenerErrors.inc(); // Count malformed payloads as an error type
    return;
  }

  // Basic validation for expected event structure
  if (typeof ev !== 'object' || ev === null || typeof ev.event_type !== 'string' || typeof ev.task_id !== 'string') {
    logger.warn({ eventReceived: ev }, 'Malformed or incomplete event structure in notification - skipping');
    listenerErrors.inc(); // Count malformed structure as an error type
    return;
  }
  
  const status = ev.event_type ?? ev.state ?? ev.type; // accept both

  if (!['DONE', 'FAILED'].includes(status)) {
    logger.trace({ status, task_id: ev.task_id }, 'Ignoring event status for rail event');
    return;
  }

  if (!USE_DAG_RUNNER) {
    logger.debug({ task_id: ev.task_id, status }, 'DAG_RUNNER_ACTIVE is false - ignoring rail event');
    return;
  }

  const wfId = `rail-event-dag-${ev.task_id}-v1`;
  logger.info({ wfId, task_id: ev.task_id, node_id: ev.node_id, status }, 'Preparing to send Temporal signal for rail event');

  if (!temporalClient) {
    try {
      temporalClient = await getTemporalClient(); // Use the new getTemporalClient function
      logger.info('Temporal client initialized.');
    } catch (err) {
      logger.error({ err }, 'Failed to initialize Temporal client');
      listenerErrors.inc();
      return; // Do not proceed if Temporal client fails
    }
  }

  try {
    logger.info({ wfId, taskQueue: 'dag-runner', signal: 'nodeDone', payload: ev }, 'Signaling workflow with start');
    
    // Ensure temporalClient is defined before use
    if (!temporalClient) {
      logger.error({ wfId }, 'Temporal client not initialized. Cannot signal workflow.');
      listenerErrors.inc();
      return; // Exit if client is not available
    }

    await temporalClient.signalWithStart(wfId, { 
      signal: 'nodeDone', // Signal name in the options object
      taskQueue: 'dag-runner',
      workflowId: wfId, // Can be redundant but allowed; ensures workflowId is used
      args: [ev],
      workflowIdReusePolicy: 'ALLOW_DUPLICATE_FAILED_ONLY',
    });
    logger.info({ wfId }, 'Successfully signaled workflow with start');
  } catch (err) {
    listenerErrors.inc();
    logger.error({ err, wfId, task_id: ev.task_id, node_id: ev.node_id }, 'Temporal signalWithStart failed for rail event');
    // Consider if specific error types from Temporal need different handling or retries here.
  }
}

// Initial boot attempt
logger.info('Starting rail-events-listener...');
bootListener().catch(initialBootError => {
  listenerErrors.inc();
  logger.fatal(initialBootError, 'Failed to boot rail-events-listener on initial attempt. Exiting.');
  process.exit(1);
});
