import 'dotenv/config'; // Ensure env vars are loaded first
import type { Notification, PoolClient } from 'pg';
import retry from 'p-retry';
import {
  collectDefaultMetrics,
  Counter,
} from 'prom-client';
// import pino from 'pino'; // Replaced with console
import { getTemporalClient, closeTemporalClient } from './temporalClient.js'; // Ensure .js extension
import type { WorkflowClient } from '@temporalio/client';
import { WorkflowIdReusePolicy } from '@temporalio/common'; // Import enum for type safety
import { pool, closePool, STATEMENT_TIMEOUT_MS, IDLE_TX_TIMEOUT_MS } from './pg.js'; // Ensure .js extension

// const pinoInstance = pino as any; // Cast to any to bypass type checking for this line
// export const logger = pinoInstance({ level: process.env.LOG_LEVEL || 'info' });
// Replaced pino with console logging facade for easier search/replace, can be more refined
export const logger = {
  info: (...args: any[]) => console.log(...args),
  warn: (...args: any[]) => console.warn(...args),
  error: (...args: any[]) => console.error(...args),
  debug: (...args: any[]) => console.log(...args), // Or console.debug if preferred and enabled
  trace: (...args: any[]) => console.log(...args), // Or console.trace
  fatal: (...args: any[]) => console.error(...args),
};


collectDefaultMetrics({ prefix: 'rail_events_listener_' });

const CHANNEL = process.env.PG_CHANNEL || 'rail_events';
const USE_DAG_RUNNER = process.env.DAG_RUNNER === 'true'; // Corrected from DAG_RUNNER_ACTIVE

const listenerErrors = new Counter({
  name: 'busywork_listener_errors_total',
  help: 'Unhandled errors in rail-events-listener',
});

const notificationsProcessed = new Counter({
  name: 'rail_events_listener_notifications_processed_total',
  help: 'Total notifications processed by the rail events listener',
});

let temporalClient: WorkflowClient | undefined;
let activeListenerClient: PoolClient | undefined; // Keep track of the active listener client for graceful shutdown

/** Boot a dedicated, long-lived LISTEN socket. */
export async function bootListener(): Promise<void> {
  logger.info('Attempting to connect to PostgreSQL for LISTEN...');
  // Ensure any previously active client is released before trying to get a new one
  if (activeListenerClient) {
    try {
      activeListenerClient.release();
      logger.info('Previous listener client released.');
    } catch (e) {
      logger.warn({error: e}, 'Error releasing previous listener client');
    }
    activeListenerClient = undefined;
  }

  const client = await pool.connect(); 
  activeListenerClient = client; // Store the successfully connected client
  logger.info(`Successfully connected client from shared pool for LISTEN. Default session statement_timeout: ${STATEMENT_TIMEOUT_MS}ms, idle_in_transaction_session_timeout: ${IDLE_TX_TIMEOUT_MS}ms`);

  try {
    // Crucially, disable server-side timeouts on THIS SPECIFIC socket from the shared pool
    logger.debug('Setting session timeouts to 0 for dedicated LISTEN client...');
    await client.query(`
      SET statement_timeout TO 0;
      SET idle_in_transaction_session_timeout TO 0;
      SET client_min_messages TO WARNING; -- Reduce verbosity of some logs
    `);
    logger.debug('Session timeouts for LISTEN client set to 0.');

    client.on('error', (err: Error) => {
      listenerErrors.inc();
      logger.error(err, 'PostgreSQL client error on LISTEN connection (will attempt to reconnect if connection ends)');
      if (activeListenerClient && activeListenerClient === client) { // ensure it's the current one and defined
        activeListenerClient.release(err); // Release with error to remove from pool
        activeListenerClient = undefined;
      }
    });

    client.on('notification', (msg: Notification) =>
      handleNotification(msg).catch(err => {
        logger.error({ err, msgPayload: msg.payload }, 'Error in handleNotification');
        listenerErrors.inc(); 
      }),
    );

    await client.query(`LISTEN ${CHANNEL}`);
    logger.info(`LISTENING on PostgreSQL channel: ${CHANNEL}`);

    client.on('end', () => {
      logger.warn('PostgreSQL client LISTEN connection ended. Initiating reconnection sequence.');
      if (activeListenerClient === client) { 
        activeListenerClient = undefined; 
      }
      
      retry(bootListener, {
        retries: process.env.LISTENER_RECONNECT_RETRIES ? parseInt(process.env.LISTENER_RECONNECT_RETRIES, 10) : 10, 
        minTimeout: 1_000,
        maxTimeout: 45_000, 
        factor: 2.5,        
        onFailedAttempt: error => {
          logger.warn(
            { attemptNumber: error.attemptNumber, retriesLeft: error.retriesLeft, error: error.message },
            'Listener reconnect attempt failed. Retrying...'
          );
        },
      }).catch(finalError => {
        logger.fatal(finalError, 'All listener reconnection attempts failed. The rail-events-listener will now exit.');
        shutdownGracefully().then(() => process.exit(1));
      });
    });
  } catch (err: any) { 
    console.error(' PG connection/SET/LISTEN failed →', JSON.stringify(err, null, 2));
    
    logger.error(
      {
        message: err?.message,
        stack: err?.stack,
        code: err?.code, 
        errno: err?.errno,
        syscall: err?.syscall,
        address: err?.address,
        port: err?.port,
        fullError: err 
      },
      'Failed to connect or setup LISTEN client'
    );
    if (activeListenerClient) { // Check if activeListenerClient is defined before releasing
      activeListenerClient.release(true); 
      activeListenerClient = undefined;
    }
    throw err; // Rethrow to be handled by p-retry in the initial call
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
    listenerErrors.inc(); 
    return;
  }

  if (typeof ev !== 'object' || ev === null || typeof ev.event_type !== 'string' || typeof ev.task_id !== 'string') {
    logger.warn({ eventReceived: ev }, 'Malformed or incomplete event structure in notification - skipping');
    listenerErrors.inc(); 
    return;
  }
  
  const status = ev.event_type ?? ev.state ?? ev.type; 

  if (!['DONE', 'FAILED'].includes(status)) {
    logger.trace({ status, task_id: ev.task_id }, 'Ignoring event status for rail event');
    return;
  }

  if (!USE_DAG_RUNNER) {
    logger.debug({ task_id: ev.task_id, status }, 'DAG_RUNNER is false - ignoring rail event');
    return;
  }

  const wfId = `rail-event-dag-${ev.task_id}-v1`;
  logger.info({ wfId, task_id: ev.task_id, node_id: ev.node_id, status }, 'Preparing to send Temporal signal for rail event');

  if (!temporalClient) {
    try {
      temporalClient = await getTemporalClient(); 
      logger.info('Temporal client initialized.');
    } catch (err) {
      logger.error({ err }, 'Failed to initialize Temporal client');
      listenerErrors.inc();
      return; 
    }
  }

  try {
    logger.info({ wfId, taskQueue: 'dag-runner', signal: 'nodeDone', payload: ev }, 'Signaling workflow with start');
    
    if (!temporalClient) {
      logger.error({ wfId }, 'Temporal client not initialized. Cannot signal workflow.');
      listenerErrors.inc();
      return; 
    }

    await temporalClient.signalWithStart(
      'main',                    // ① workflow type exported by dagRunner.workflow.ts
      {
        /* ---------- start-workflow section ---------- */
        args: [{ taskId: ev.task_id }],      // <- first (and only) arg to main()

        workflowId: wfId,                    // same ID every time for this task
        taskQueue: 'dag-runner',

        /* ---------- immediate signal section ---------- */
        signal: 'nodeDone',
        signalArgs: [ev],                    // the NodeDoneSignal payload

        workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
      }
    );
    logger.info({ wfId }, 'Successfully signaled workflow with start');
    notificationsProcessed.inc(); 
  } catch (err) {
    logger.error({ err, wfId }, 'Failed to signal workflow with start');
    listenerErrors.inc();
  }
}

async function shutdownGracefully(signal?: string) {
  if (signal) {
    logger.info(`Received ${signal}. Shutting down gracefully...`);
  }
  if (activeListenerClient) {
    try {
      logger.info('Releasing active listener client...');
      activeListenerClient.release();
      activeListenerClient = undefined;
      logger.info('Active listener client released.');
    } catch (e) {
      logger.error({error: e}, 'Error releasing active listener client during shutdown.');
    }
  }
  // Close pg pool
  try {
    logger.info('Closing PostgreSQL pool...');
    await closePool();
    logger.info('PostgreSQL pool closed.');
  } catch (e) {
    logger.error({error: e}, 'Error closing PostgreSQL pool during shutdown.');
  }
  // Close Temporal client
  try {
    logger.info('Closing Temporal client...');
    await closeTemporalClient();
    logger.info('Temporal client closed.');
  } catch (e) {
    logger.error({error: e}, 'Error closing Temporal client during shutdown.');
  }
  // Any other cleanup
  logger.info('Graceful shutdown complete.');
}

process.on('SIGINT', async () => {
  await shutdownGracefully('SIGINT');
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await shutdownGracefully('SIGTERM');
  process.exit(0);
});
