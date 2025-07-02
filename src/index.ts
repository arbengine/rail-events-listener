// rail-events-listener/src/index.ts
// 🔄 CLEAN RAIL EVENTS LISTENER - Works with Clean Architecture
// 🤝 PERFECT INTEGRATION: Handles intelligence → segmentation → DAG creation flow
// 📡 ENHANCED NOTIFICATIONS: Routes events between clean separated workers
// ⚡ SCALABLE: Handles complex conditional logic notifications
// 🏗️ ARCHITECTURE: Pure event routing and transformation
// -------------------------------------------------------------------------------

import 'dotenv/config';
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

/* ─── ENHANCED: NATS client for clean architecture ──────────────────────────────── */
import { connect as natsConnect, StringCodec } from 'nats';

const nc = await natsConnect({
  servers: process.env.NATS_URL || 'nats://nats-scalable:4222',
});
const sc = StringCodec();

console.log('[rail-events] ✅ connected to NATS for clean architecture');

import { RailEvent, toDelta, BroadcastDelta } from './utils/delta.js';
import { initializeWebSocketServer, broadcast, closeWebSocketServer } from './websocketServer.js';

// Enhanced logger for clean architecture
export const logger = {
  info : (...a: any[]) => console.log(...a),
  warn : (...a: any[]) => console.warn(...a),
  error: (...a: any[]) => console.error(...a),
  debug: (...a: any[]) => console.debug(...a),
  trace: (...a: any[]) => console.trace(...a),
  fatal: (...a: any[]) => console.error(...a),
};

// Enhanced metrics for clean architecture
collectDefaultMetrics({ prefix: 'rail_events_listener_clean_' });

const lastEventByNode = new Map<string, RailEvent>();

const CHANNEL        = process.env.PG_CHANNEL || 'rail_event';
const USE_DAG_RUNNER = process.env.DAG_RUNNER === 'true';
const INSTANCE_ID    = process.env.HOSTNAME || 'clean-architecture';

const listenerErrors = new Counter({
  name: 'busywork_clean_listener_errors_total',
  help: 'Unhandled errors in clean rail-events-listener',
  labelNames: ['instance_id', 'error_type'],
});

const notificationsProcessed = new Counter({
  name: 'rail_events_clean_notifications_processed_total',
  help: 'Total notifications processed in clean architecture',
  labelNames: ['status', 'instance_id', 'workflow_stage'],
});

// Enhanced notification counters for clean architecture
const intelligenceNotifications = new Counter({
  name: 'busywork_intelligence_notifications_total',
  help: 'Intelligence analysis notifications processed',
  labelNames: ['instance_id'],
});

const segmentationNotifications = new Counter({
  name: 'busywork_segmentation_notifications_total',
  help: 'Segmentation notifications processed',
  labelNames: ['instance_id'],
});

const dagCreationNotifications = new Counter({
  name: 'busywork_dag_creation_notifications_total',
  help: 'DAG creation notifications processed',
  labelNames: ['instance_id'],
});

const logCtx = (extra: Record<string, any> = {}) => ({ 
  instance_id: INSTANCE_ID, 
  architecture: 'clean_separation',
  ...extra 
});

let temporalClient: WorkflowClient | undefined;
let activeListenerClient: PoolClient | undefined;

// Enhanced listener for clean architecture
export async function bootCleanListener(): Promise<void> {
  logger.info('🚀 Attempting to connect to PostgreSQL for clean architecture LISTEN…');

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
      clean_architecture: true
    }),
    '✅ Connected to PG for clean architecture LISTEN',
  );

  await client.query(`SET statement_timeout TO 0; SET idle_in_transaction_session_timeout TO 0; SET client_min_messages TO WARNING;`);
  logger.debug('Session timeouts set for clean architecture LISTEN socket');

  client.on('error', (err: Error) => {
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'connection' });
    logger.error(logCtx({ err }), '💥 PostgreSQL clean architecture LISTEN client error — reconnecting');
    try { client.release(err); } catch {}
    activeListenerClient = undefined;
  });

  client.on('notification', (msg: Notification) =>
    handleCleanArchitectureNotification(msg).catch((err) => {
      logger.error(logCtx({ err, payload: msg.payload }), '💥 Error in clean architecture handleNotification');
      listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'notification_handling' });
    }),
  );

  // Listen to multiple channels for clean architecture
  await client.query(`LISTEN ${CHANNEL}`);
  await client.query(`LISTEN task_ready_for_dag_generation`);
  await client.query(`LISTEN intelligent_segmentation_complete`);
  await client.query(`LISTEN dag_creation_complete`);
  
  logger.info(logCtx({ 
    channels: [CHANNEL, 'task_ready_for_dag_generation', 'intelligent_segmentation_complete', 'dag_creation_complete']
  }), '🔔 LISTENING for clean architecture events');

  /* Enhanced heartbeat for clean architecture */
  const heartbeat = setInterval(async () => {
    try {
      await query('SELECT 1');
      logger.info(logCtx(), '❤️ clean architecture listener heartbeat OK');
    } catch (err: any) {
      logger.warn(logCtx({ err }), '💔 clean architecture heartbeat failed – connection likely lost');
    }
  }, 30_000);

  client.once('end', () => clearInterval(heartbeat));
}

// Enhanced notification handler for clean architecture
async function handleCleanArchitectureNotification(msg: Notification): Promise<void> {
  if (!msg.payload) return;

  logger.debug(logCtx({ channel: msg.channel }), `🔔 Clean architecture notification received on channel: ${msg.channel}`);

  /* Route to appropriate handler based on channel */
  switch (msg.channel) {
    case CHANNEL:
      await handleStandardRailEvent(msg);
      break;
    case 'task_ready_for_dag_generation':
      await handleIntelligenceNotification(msg);
      break;
    case 'intelligent_segmentation_complete':
      await handleSegmentationNotification(msg);
      break;
    case 'dag_creation_complete':
      await handleDAGCreationNotification(msg);
      break;
    default:
      logger.warn(logCtx({ channel: msg.channel }), 'Unknown notification channel in clean architecture');
  }
}

// Standard rail event handler (existing functionality)
async function handleStandardRailEvent(msg: Notification): Promise<void> {
  if (msg.channel !== CHANNEL || !msg.payload) return;

  /* Parse JSON with clean architecture enhancements */
  let raw: any;
  try {
    raw = JSON.parse(msg.payload);

    /* Enhanced normalization for clean architecture */
    if (raw.taskId        && !raw.task_id)       raw.task_id       = raw.taskId;
    if (raw.nodeId        && !raw.node_id)       raw.node_id       = raw.nodeId;
    if (raw.eventSubtype  && !raw.event_subtype) raw.event_subtype = raw.eventSubtype;

    /* Clean architecture: enhanced key trimming */
    for (const k of Object.keys(raw)) {
      const trimmed = k.trim();
      if (trimmed !== k && raw[trimmed] === undefined) {
        raw[trimmed] = raw[k];
        delete raw[k];
      }
    }
  } catch (err) {
    logger.error(logCtx({ err, payload: msg.payload }), 'Failed JSON.parse in clean architecture');
    return;
  }

  /* Enhanced shape validation for clean architecture */
  if (typeof raw !== 'object' || !raw.task_id || !raw.node_id || !raw.state) {
    logger.warn(logCtx({ payload: msg.payload, normalised: raw }),
                'Payload does not conform to clean architecture RailEvent');
    return;
  }

  const curr: RailEvent = raw as RailEvent;
  const key = `${curr.task_id}:${curr.node_id}`;
  const prev = lastEventByNode.get(key) ?? null;
  const delta: BroadcastDelta = toDelta(curr!, prev);

  /* Enhanced NATS publishing for clean architecture */
  if (delta.state === 'WAITING_AI') {
    const { rows: [node] } = await query(
      `SELECT pending_instructions_md AS md, metadata
         FROM execution_nodes
        WHERE node_id = $1`,
      [delta.nodeId],
    );

    const payload = {
      taskId : delta.taskId,
      nodeId : delta.nodeId,
      attempt: 1,
      md     : node?.md ?? null,
      metadata: node?.metadata || {},
      architecture: 'clean_separation',
      timestamp: new Date().toISOString()
    };

    const subj = `busywork.node.ready.${delta.nodeId}`;
    nc.publish(subj, sc.encode(JSON.stringify(payload)));
    logger.debug(logCtx({ node: delta.nodeId }), '📤 Clean architecture NATS busywork.node.ready published');
  }

  // Enhanced WebSocket broadcast for clean architecture
  broadcast({
    ...delta,
    architecture: 'clean_separation',
    timestamp: new Date().toISOString()
  });

  /* Enhanced NATS publishing for node completion */
  if (curr.state === 'DONE') {
    try {
      await nc.publish(
        `busywork.node.done.${curr.node_id}`,
        sc.encode(JSON.stringify({
          taskId: curr.task_id,
          nodeId: curr.node_id,
          architecture: 'clean_separation',
          timestamp: new Date().toISOString()
        })),
      );
      logger.debug(logCtx({ node: curr.node_id }), '📤 Clean architecture NATS busywork.node.done published');
    } catch (err) {
      logger.error(logCtx({ err }), '💥 Clean architecture NATS publish failed (non-fatal)');
    }
  }

  logger.info({ 
    deltaPayload: true, 
    delta, 
    taskId: curr.task_id, 
    nodeId: curr.node_id,
    architecture: 'clean_separation'
  });

  lastEventByNode.set(key, curr);

  // Enhanced Temporal workflow handling for clean architecture
  const statusForTemporal = curr.state;
  if (!['DONE', 'FAILED'].includes(statusForTemporal)) {
    logger.debug(logCtx({ task_id: curr.task_id, status: statusForTemporal }), 
                'Skipping Temporal signal for non-terminal status in clean architecture');
    return;
  }

  if (!USE_DAG_RUNNER) {
    logger.debug(logCtx({ task_id: curr.task_id, status: statusForTemporal }), 
                '⏭️ DAG_RUNNER=false — skipping event in clean architecture');
    return;
  }

  const wfId = `rail-event-dag-${curr.task_id}-v1`;
  logger.info(logCtx({ wfId, node_id: curr.node_id, status: statusForTemporal }), 
             '📤 Preparing to signal Temporal workflow in clean architecture…');

  if (!temporalClient) temporalClient = await getTemporalClient();

  await temporalClient.signalWithStart('main', {
    args: [{ taskId: curr.task_id }],
    workflowId: wfId,
    taskQueue: 'dag-runner',
    signal: 'nodeDone',
    signalArgs: [curr],
    workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
  });

  notificationsProcessed.inc({ 
    status: statusForTemporal, 
    instance_id: INSTANCE_ID,
    workflow_stage: 'standard_rail_event'
  });
  logger.info(logCtx({ wfId }), '✅ Clean architecture Temporal workflow signaled successfully');
}

// New: Intelligence notification handler
async function handleIntelligenceNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const taskId = payload.task_id;
    
    logger.info(logCtx({ taskId }), '🧠 Intelligence notification received - routing to segmentation');
    
    // Intelligence analysis complete, route to segmentation engine
    intelligenceNotifications.inc({ instance_id: INSTANCE_ID });
    
    // The segmentation engine will pick this up via its own listener
    // This is just for monitoring and WebSocket updates
    broadcast({
      v: 1,
      taskId,
      nodeId: 'intelligence_analysis',
      state: 'intelligence_complete',
      title: 'Intelligence Analysis Complete',
      architecture: 'clean_separation',
      workflow_stage: 'intelligence_analysis',
      timestamp: new Date().toISOString()
    });
    
    logger.info(logCtx({ taskId }), '✅ Intelligence notification processed and broadcasted');
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling intelligence notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'intelligence_notification' });
  }
}

// New: Segmentation notification handler
async function handleSegmentationNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const taskId = payload.task_id;
    
    logger.info(logCtx({ taskId }), '🎯 Segmentation notification received - routing to template hydrator');
    
    // Segmentation complete, route to template hydrator
    segmentationNotifications.inc({ instance_id: INSTANCE_ID });
    
    broadcast({
      v: 1,
      taskId,
      nodeId: 'segmentation_analysis',
      state: 'segmentation_complete',
      title: 'Task Segmentation Complete',
      architecture: 'clean_separation',
      workflow_stage: 'segmentation',
      timestamp: new Date().toISOString()
    });
    
    logger.info(logCtx({ taskId }), '✅ Segmentation notification processed and broadcasted');
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling segmentation notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'segmentation_notification' });
  }
}

// New: DAG creation notification handler
async function handleDAGCreationNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const taskId = payload.task_id;
    const nodesCreated = payload.nodes_created || 0;
    
    logger.info(logCtx({ taskId, nodesCreated }), '🏗️ DAG creation notification received - workflow complete');
    
    // DAG creation complete - final stage
    dagCreationNotifications.inc({ instance_id: INSTANCE_ID });
    
    broadcast({
      v: 1,
      taskId,
      nodeId: 'dag_creation',
      state: 'dag_complete',
      title: `DAG Created (${nodesCreated} nodes)`,
      architecture: 'clean_separation',
      workflow_stage: 'dag_creation',
      nodesCreated,
      timestamp: new Date().toISOString()
    });
    
    // Update task status in database
    await query(
      `UPDATE tasks 
       SET status = 'hydrated', 
           updated_at = NOW(),
           routing_metadata = routing_metadata || $2::jsonb
       WHERE task_id = $1`,
      [taskId, JSON.stringify({
        clean_architecture_complete: true,
        dag_creation_timestamp: new Date().toISOString(),
        nodes_created: nodesCreated,
        workflow_stage: 'complete'
      })]
    );
    
    logger.info(logCtx({ taskId, nodesCreated }), '✅ DAG creation notification processed - workflow complete');
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling DAG creation notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'dag_creation_notification' });
  }
}

// Enhanced graceful shutdown for clean architecture
async function shutdownCleanArchitecture(reason?: string) {
  logger.info(logCtx({ reason }), '🛑 Clean architecture graceful shutdown requested');

  await new Promise<void>(resolve => closeWebSocketServer(resolve));
  try { await closeTemporalClient(); } catch {} 
  try { await closePool(); } catch {} 
  if (activeListenerClient) {
    try { activeListenerClient.release(); } catch {}
  }
  try { await nc.close(); } catch {}

  logger.info(logCtx({
    metrics: {
      processed: notificationsProcessed.get(),
      errors: listenerErrors.get(),
      intelligence: intelligenceNotifications.get(),
      segmentation: segmentationNotifications.get(),
      dag_creation: dagCreationNotifications.get()
    }
  }), '📊 Clean architecture shutdown complete with final metrics');
}

process.on('SIGINT', () => shutdownCleanArchitecture('SIGINT').then(() => process.exit(0)));
process.on('SIGTERM', () => shutdownCleanArchitecture('SIGTERM').then(() => process.exit(0)));

// Enhanced bootstrap for clean architecture
import pRetry from 'p-retry';

(async () => {
  logger.info(logCtx(), '🚀 Starting clean architecture rail-events-listener bootstrap…');
  logger.info(logCtx({ USE_DAG_RUNNER }), `🚦 DAG_RUNNER = ${USE_DAG_RUNNER}`);
  logger.info(logCtx({ 
    channels: [CHANNEL, 'task_ready_for_dag_generation', 'intelligent_segmentation_complete', 'dag_creation_complete']
  }), `📡 Subscribing to clean architecture channels`);

  try {
    await pRetry(bootCleanListener, { retries: 5, minTimeout: 1_000, factor: 2 });
    logger.info(logCtx(), '✅ Clean architecture PostgreSQL listener booted');
    
    temporalClient = await getTemporalClient();
    logger.info(logCtx(), '✅ Clean architecture Temporal client ready');
    
    initializeWebSocketServer(logger);
    logger.info(logCtx(), '✅ Clean architecture WebSocket server ready');
    
    logger.info(logCtx({
      architecture: 'clean_separation',
      components: ['routing_worker', 'segmentation_engine', 'template_hydrator'],
      workflow: 'intelligence → segmentation → dag_creation',
      features: ['conditional_logic', 'business_intelligence', 'human_logical_nodes']
    }), '🎉 Clean architecture application started successfully and is listening for events.');
    
  } catch (err) {
    logger.fatal(logCtx({ err }), '💥 Failed to start clean architecture listener');
    await shutdownCleanArchitecture('startup failure');
    process.exit(1);
  }
})();