// rail-events-listener/src/index.ts
// 🔧 FIXED: Rail Events Listener with SSL bypass (same as other workers)
// 🔔 NOTIFICATIONS: Catches and handles all worker notifications  
// ⚡ REAL-TIME: Event processing for clean architecture
// 🛡️ SSL BYPASS: Same approach as routing worker and template hydrator
// -----------------------------------------------------------------------------

// 🛡️ SSL BYPASS: Same as your other workers
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

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

/* ─── NATS client ──────────────────────────────── */
import { connect as natsConnect, StringCodec, NatsConnection } from 'nats';

let natsConnection: NatsConnection | null = null;
const sc = StringCodec();

async function getNatsConnection(): Promise<NatsConnection> {
  if (!natsConnection || natsConnection.isClosed()) {
    try {
      natsConnection = await natsConnect({
        servers: process.env.NATS_URL || 'nats://nats-scalable:4222',
        reconnect: true,
        maxReconnectAttempts: -1,
        reconnectTimeWait: 1000,
      });
      logger.info('✅ Connected/reconnected to NATS for clean architecture');
    } catch (error) {
      logger.error('❌ Failed to connect to NATS:', error);
      throw error;
    }
  }
  return natsConnection;
}

import { RailEvent, toDelta, BroadcastDelta } from './utils/delta.js';
import { initializeWebSocketServer, broadcast, closeWebSocketServer } from './websocketServer.js';

// Logger
export const logger = {
  info : (...a: any[]) => console.log(new Date().toISOString(), '[INFO]', ...a),
  warn : (...a: any[]) => console.warn(new Date().toISOString(), '[WARN]', ...a),
  error: (...a: any[]) => console.error(new Date().toISOString(), '[ERROR]', ...a),
  debug: (...a: any[]) => console.debug(new Date().toISOString(), '[DEBUG]', ...a),
  trace: (...a: any[]) => console.trace(new Date().toISOString(), '[TRACE]', ...a),
  fatal: (...a: any[]) => console.error(new Date().toISOString(), '[FATAL]', ...a),
};

// Metrics
collectDefaultMetrics({ prefix: 'rail_events_listener_fixed_' });

const lastEventByNode = new Map<string, RailEvent>();

const CHANNEL        = process.env.PG_CHANNEL || 'rail_event';
const USE_DAG_RUNNER = process.env.DAG_RUNNER === 'true';
const INSTANCE_ID    = process.env.HOSTNAME || 'fixed-rail-listener';

// Connection tracking
let activeListenerClient: PoolClient | undefined;
let isShuttingDown = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
let heartbeatInterval: NodeJS.Timeout | null = null;

const listenerErrors = new Counter({
  name: 'busywork_fixed_listener_errors_total',
  help: 'Unhandled errors in fixed rail-events-listener',
  labelNames: ['instance_id', 'error_type'],
});

const notificationsReceived = new Counter({
  name: 'rail_events_fixed_notifications_received_total',
  help: 'Total notifications received in fixed architecture',
  labelNames: ['channel', 'instance_id'],
});

const notificationsProcessed = new Counter({
  name: 'rail_events_fixed_notifications_processed_total',
  help: 'Total notifications processed in fixed architecture',
  labelNames: ['status', 'instance_id', 'workflow_stage'],
});

const connectionEvents = new Counter({
  name: 'rail_events_fixed_connection_events_total',
  help: 'Connection events in fixed rail listener',
  labelNames: ['event_type', 'instance_id'],
});

const logCtx = (extra: Record<string, any> = {}) => ({ 
  instance_id: INSTANCE_ID, 
  architecture: 'fixed_clean_separation',
  timestamp: new Date().toISOString(),
  ...extra 
});

let temporalClient: WorkflowClient | undefined;

// 🔧 FIXED: Simple listener boot with SSL bypass
export async function bootFixedListener(): Promise<void> {
  if (isShuttingDown) return;
  
  logger.info(logCtx(), '🚀 Attempting to connect to PostgreSQL for FIXED LISTEN...');

  // Clean up existing connection
  if (activeListenerClient) {
    try {
      logger.debug(logCtx(), '🧹 Cleaning up existing listener connection');
      activeListenerClient.removeAllListeners();
      activeListenerClient.release();
      connectionEvents.inc({ event_type: 'connection_cleanup', instance_id: INSTANCE_ID });
    } catch (error) {
      logger.warn(logCtx({ error }), '⚠️ Error during connection cleanup');
    }
    activeListenerClient = undefined;
  }

  try {
    const client = await pool.connect();
    activeListenerClient = client;
    
    logger.info(logCtx({
      statement_timeout: STATEMENT_TIMEOUT_MS,
      idle_tx_timeout: IDLE_TX_TIMEOUT_MS,
      reconnect_attempt: reconnectAttempts + 1
    }), '✅ Connected to PostgreSQL for FIXED LISTEN');

    // Session configuration (same as your other workers)
    await client.query(`
      SET statement_timeout TO 0;
      SET idle_in_transaction_session_timeout TO 0;
      SET client_min_messages TO WARNING;
    `);
    
    logger.debug(logCtx(), '⚙️ Session configured for FIXED LISTEN');

    // Error handling
    client.on('error', async (err: Error) => {
      connectionEvents.inc({ event_type: 'connection_error', instance_id: INSTANCE_ID });
      listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'connection' });
      logger.error(logCtx({ error: err.message }), '💥 PostgreSQL FIXED LISTEN client error');
      
      activeListenerClient = undefined;
      
      if (!isShuttingDown) {
        logger.info(logCtx(), '🔄 Scheduling reconnection after error...');
        setTimeout(() => {
          if (!isShuttingDown) {
            bootFixedListener().catch(error => {
              logger.error(logCtx({ error }), '❌ Reconnection attempt failed');
            });
          }
        }, 2000);
      }
    });

    client.on('end', () => {
      connectionEvents.inc({ event_type: 'connection_end', instance_id: INSTANCE_ID });
      logger.warn(logCtx(), '🔌 PostgreSQL FIXED connection ended');
      activeListenerClient = undefined;
      
      if (!isShuttingDown) {
        logger.info(logCtx(), '🔄 Connection ended - scheduling reconnection...');
        setTimeout(() => {
          if (!isShuttingDown) {
            bootFixedListener().catch(error => {
              logger.error(logCtx({ error }), '❌ Reconnection after end failed');
            });
          }
        }, 1000);
      }
    });

    // Notification handler
    client.on('notification', async (msg: Notification) => {
      try {
        notificationsReceived.inc({ channel: msg.channel, instance_id: INSTANCE_ID });
        logger.info(logCtx({ 
          channel: msg.channel, 
          payload_length: msg.payload?.length || 0,
        }), '🔔 NOTIFICATION RECEIVED');
        
        await handleFixedNotification(msg);
        
        logger.debug(logCtx({ channel: msg.channel }), '✅ Notification processed successfully');
      } catch (error) {
        logger.error(logCtx({ 
          error: error instanceof Error ? error.message : String(error), 
          channel: msg.channel,
        }), '💥 Error in FIXED notification handler');
        listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'notification_handling' });
      }
    });

    // Listen to ALL channels that workers use
    const channels = [
      CHANNEL,                              // Standard rail_event
      'task_ready_for_dag_generation',      // Intelligence → Segmentation
      'intelligent_segmentation_complete',   // Segmentation → Template Hydrator
      'dag_creation_complete',              // Template Hydrator → Complete
      'nats_bridge',                        // NATS bridge notifications
      'worker_status',                      // Worker status updates
      'template_hydrated',                  // Template hydration complete
      'task_routed',                        // Task routing notifications
      'node_state_change',                  // Direct node state changes
      'dynamic_node_spawned',               // Dynamic node spawning notifications
    ];
    
    for (const channel of channels) {
      await client.query(`LISTEN ${channel}`);
      logger.debug(logCtx({ channel }), '👂 Listening to channel');
    }
    
    logger.info(logCtx({ 
      channels_count: channels.length,
      channels: channels
    }), '🔔 LISTENING for FIXED events on ALL channels');

    // Start heartbeat
    startFixedHeartbeat();
    
    // Reset reconnect attempts on successful connection
    reconnectAttempts = 0;
    connectionEvents.inc({ event_type: 'successful_connection', instance_id: INSTANCE_ID });
    
  } catch (error) {
    connectionEvents.inc({ event_type: 'connection_failure', instance_id: INSTANCE_ID });
    logger.error(logCtx({ 
      error: error instanceof Error ? error.message : String(error),
      reconnect_attempt: reconnectAttempts + 1
    }), '❌ Failed to establish FIXED LISTEN connection');
    
    reconnectAttempts++;
    
    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS && !isShuttingDown) {
      const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 30000);
      logger.info(logCtx({ 
        delay_ms: delay, 
        attempt: reconnectAttempts, 
        max_attempts: MAX_RECONNECT_ATTEMPTS 
      }), '🔄 Scheduling reconnection...');
      
      setTimeout(() => {
        if (!isShuttingDown) {
          bootFixedListener().catch(error => {
            logger.error(logCtx({ error }), '❌ Scheduled reconnection failed');
          });
        }
      }, delay);
    } else {
      logger.fatal(logCtx({ max_attempts_reached: true }), '💀 Max reconnection attempts reached');
      process.exit(1);
    }
  }
}

// Heartbeat
function startFixedHeartbeat() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }
  
  heartbeatInterval = setInterval(async () => {
    if (!activeListenerClient || isShuttingDown) {
      return;
    }
    
    try {
      await activeListenerClient.query('SELECT 1 as heartbeat');
      logger.debug(logCtx(), '❤️ FIXED heartbeat OK');
    } catch (error) {
      logger.error(logCtx({ 
        error: error instanceof Error ? error.message : String(error) 
      }), '💔 FIXED heartbeat failed');
      
      connectionEvents.inc({ event_type: 'heartbeat_failure', instance_id: INSTANCE_ID });
      
      if (activeListenerClient) {
        try {
          activeListenerClient.removeAllListeners();
          activeListenerClient.release();
        } catch (cleanupError) {
          // Ignore cleanup errors
        }
        activeListenerClient = undefined;
      }
      
      if (!isShuttingDown) {
        setTimeout(() => {
          if (!isShuttingDown) {
            bootFixedListener().catch(error => {
              logger.error(logCtx({ error }), '❌ Heartbeat-triggered reconnection failed');
            });
          }
        }, 1000);
      }
    }
  }, 30000); // Check every 30 seconds
}

// Notification router
async function handleFixedNotification(msg: Notification): Promise<void> {
  if (!msg.payload) {
    logger.warn(logCtx({ channel: msg.channel }), '⚠️ Received notification with empty payload');
    return;
  }

  logger.info(logCtx({ 
    channel: msg.channel,
    payload_length: msg.payload.length,
  }), '🔄 Processing notification...');

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
    case 'nats_bridge':
      await handleNatsBridgeNotification(msg);
      break;
    case 'dynamic_node_spawned':
      await handleDynamicNodeSpawnNotification(msg);
      break;
    default:
      logger.debug(logCtx({ channel: msg.channel }), '📝 Generic notification logged');
      broadcast({
        v: 1,
        taskId: 'system',
        nodeId: 'notification',
        state: 'notification_received',
        title: `${msg.channel} notification`,
        architecture: 'fixed_clean_separation',
        timestamp: new Date().toISOString()
      });
  }
}

// Standard rail event handler
async function handleStandardRailEvent(msg: Notification): Promise<void> {
  if (msg.channel !== CHANNEL || !msg.payload) return;

  logger.debug(logCtx({ channel: msg.channel }), '🚂 Processing standard rail event');

  let raw: any;
  try {
    raw = JSON.parse(msg.payload);

    // Normalize field names
    if (raw.taskId && !raw.task_id) raw.task_id = raw.taskId;
    if (raw.nodeId && !raw.node_id) raw.node_id = raw.nodeId;
    if (raw.eventSubtype && !raw.event_subtype) raw.event_subtype = raw.eventSubtype;

  } catch (err) {
    logger.error(logCtx({ 
      error: err instanceof Error ? err.message : String(err), 
      payload: msg.payload 
    }), '❌ Failed JSON.parse in FIXED rail event');
    return;
  }

  // Validate shape
  if (typeof raw !== 'object' || !raw.task_id || !raw.node_id || !raw.state) {
    logger.warn(logCtx({ 
      payload: msg.payload, 
      normalised: raw,
    }), '⚠️ Payload does not conform to RailEvent shape');
    return;
  }

  const curr: RailEvent = raw as RailEvent;
  const key = `${curr.task_id}:${curr.node_id}`;
  const prev = lastEventByNode.get(key) ?? null;
  const delta: BroadcastDelta = toDelta(curr!, prev);

  logger.info(logCtx({
    task_id: curr.task_id,
    node_id: curr.node_id,
    state: curr.state,
    event_subtype: curr.event_subtype
  }), '📊 Rail event processed - generating delta');

  // NATS publishing for WAITING_AI
  if (delta.state === 'WAITING_AI') {
    try {
      const { rows: [node] } = await query(
        `SELECT pending_instructions_md AS md, metadata
           FROM execution_nodes
          WHERE node_id = $1`,
        [delta.nodeId],
      );

      const payload = {
        taskId: delta.taskId,
        nodeId: delta.nodeId,
        attempt: 1,
        md: node?.md ?? null,
        metadata: node?.metadata || {},
        architecture: 'fixed_clean_separation',
        timestamp: new Date().toISOString(),
        source: 'rail_events_listener_fixed'
      };

      const nc = await getNatsConnection();
      const subj = `busywork.node.ready.${delta.nodeId}`;
      nc.publish(subj, sc.encode(JSON.stringify(payload)));
      
      logger.info(logCtx({ 
        node_id: delta.nodeId, 
        subject: subj 
      }), '📤 FIXED NATS busywork.node.ready published');
      
    } catch (error) {
      logger.error(logCtx({ 
        error: error instanceof Error ? error.message : String(error),
        node_id: delta.nodeId 
      }), '❌ Failed to publish NATS ready event');
    }
  }

  // WebSocket broadcast
  broadcast({
    ...delta,
    architecture: 'fixed_clean_separation',
    timestamp: new Date().toISOString(),
    source: 'rail_events_listener_fixed'
  });

  // NATS publishing for node completion
  if (curr.state === 'DONE') {
    try {
      const nc = await getNatsConnection();
      await nc.publish(
        `busywork.node.done.${curr.node_id}`,
        sc.encode(JSON.stringify({
          taskId: curr.task_id,
          nodeId: curr.node_id,
          architecture: 'fixed_clean_separation',
          timestamp: new Date().toISOString(),
          source: 'rail_events_listener_fixed'
        })),
      );
      logger.debug(logCtx({ node_id: curr.node_id }), '📤 FIXED NATS busywork.node.done published');
    } catch (err) {
      logger.error(logCtx({ error: err }), '💥 FIXED NATS publish failed (non-fatal)');
    }
  }

  lastEventByNode.set(key, curr);

  // Temporal workflow handling (if enabled)
  const statusForTemporal = curr.state;
  if (!['DONE', 'FAILED'].includes(statusForTemporal)) {
    return;
  }

  if (!USE_DAG_RUNNER) {
    logger.debug(logCtx({ task_id: curr.task_id, status: statusForTemporal }), 
                '⏭️ DAG_RUNNER=false — skipping Temporal event');
    return;
  }

  try {
    const wfId = `rail-event-dag-${curr.task_id}-v1`;
    logger.info(logCtx({ wfId, node_id: curr.node_id, status: statusForTemporal }), 
               '📤 Signaling Temporal workflow...');

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
    
    logger.info(logCtx({ wfId }), '✅ Temporal workflow signaled successfully');
  } catch (error) {
    logger.error(logCtx({ 
      error: error instanceof Error ? error.message : String(error),
      task_id: curr.task_id 
    }), '❌ Failed to signal Temporal workflow');
  }
}

// New notification handlers
async function handleIntelligenceNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const taskId = payload.task_id;
    
    logger.info(logCtx({ taskId }), '🧠 Intelligence notification received');
    
    broadcast({
      v: 1,
      taskId,
      nodeId: 'intelligence_analysis',
      state: 'intelligence_complete',
      title: 'Intelligence Analysis Complete',
      architecture: 'fixed_clean_separation',
      workflow_stage: 'intelligence_analysis',
      timestamp: new Date().toISOString()
    });
    
    notificationsProcessed.inc({ 
      status: 'intelligence_complete', 
      instance_id: INSTANCE_ID,
      workflow_stage: 'intelligence'
    });
    
    logger.info(logCtx({ taskId }), '✅ Intelligence notification processed');
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling intelligence notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'intelligence_notification' });
  }
}

async function handleSegmentationNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const taskId = payload.task_id;
    
    logger.info(logCtx({ taskId }), '🎯 Segmentation notification received');
    
    broadcast({
      v: 1,
      taskId,
      nodeId: 'segmentation_analysis',
      state: 'segmentation_complete',
      title: 'Task Segmentation Complete',
      architecture: 'fixed_clean_separation',
      workflow_stage: 'segmentation',
      timestamp: new Date().toISOString()
    });
    
    notificationsProcessed.inc({ 
      status: 'segmentation_complete', 
      instance_id: INSTANCE_ID,
      workflow_stage: 'segmentation'
    });
    
    logger.info(logCtx({ taskId }), '✅ Segmentation notification processed');
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling segmentation notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'segmentation_notification' });
  }
}

async function handleDAGCreationNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const taskId = payload.task_id;
    const nodesCreated = payload.nodes_created || 0;
    
    logger.info(logCtx({ taskId, nodesCreated }), '🏗️ DAG creation notification received');
    
    broadcast({
      v: 1,
      taskId,
      nodeId: 'dag_creation',
      state: 'dag_complete',
      title: `DAG Created (${nodesCreated} nodes)`,
      architecture: 'fixed_clean_separation',
      workflow_stage: 'dag_creation',
      nodesCreated,
      timestamp: new Date().toISOString()
    });
    
    notificationsProcessed.inc({ 
      status: 'dag_complete', 
      instance_id: INSTANCE_ID,
      workflow_stage: 'dag_creation'
    });
    
    // Update task status
    await query(
      `UPDATE tasks 
       SET status = 'hydrated', 
           updated_at = NOW(),
           routing_metadata = routing_metadata || $2::jsonb
       WHERE task_id = $1`,
      [taskId, JSON.stringify({
        fixed_architecture_complete: true,
        dag_creation_timestamp: new Date().toISOString(),
        nodes_created: nodesCreated,
        workflow_stage: 'complete'
      })]
    );
    
    logger.info(logCtx({ taskId, nodesCreated }), '✅ DAG creation notification processed');
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling DAG creation notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'dag_creation_notification' });
  }
}

async function handleNatsBridgeNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    logger.debug(logCtx({ payload }), '🌉 NATS bridge notification received');
    
    if (payload.subject && payload.payload) {
      const nc = await getNatsConnection();
      nc.publish(payload.subject, sc.encode(JSON.stringify(payload.payload)));
      logger.debug(logCtx({ subject: payload.subject }), '📤 NATS bridge message forwarded');
    }
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling NATS bridge notification');
    listenerErrors.inc({ instance_id: INSTANCE_ID, error_type: 'nats_bridge_notification' });
  }
}

async function handleDynamicNodeSpawnNotification(msg: Notification): Promise<void> {
  try {
    const payload = JSON.parse(msg.payload!);
    const { task_id, parent_id, spawned_ids } = payload;
    
    logger.info(logCtx({ 
      task_id, 
      parent_id, 
      spawned_count: spawned_ids.length 
    }), '🌟 Dynamic nodes spawned');
    
    // Broadcast to WebSocket for real-time updates
    broadcast({
      v: 1,
      taskId: task_id,
      nodeId: parent_id,
      state: 'spawned_children',
      title: `Spawned ${spawned_ids.length} dynamic nodes`,
      metadata: {
        spawned_ids,
        parent_id,
        spawn_type: 'runtime_conditional'
      },
      architecture: 'dynamic_dag',
      timestamp: new Date().toISOString()
    });
    
    notificationsProcessed.inc({ 
      status: 'dynamic_spawn', 
      instance_id: INSTANCE_ID,
      workflow_stage: 'dynamic_generation'
    });
    
  } catch (error) {
    logger.error(logCtx({ error }), '❌ Error handling dynamic spawn notification');
  }
}

// Graceful shutdown
async function shutdownFixedArchitecture(reason?: string) {
  logger.info(logCtx({ reason }), '🛑 FIXED architecture graceful shutdown initiated');
  isShuttingDown = true;

  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }

  try {
    await new Promise<void>(resolve => closeWebSocketServer(resolve));
    logger.info(logCtx(), '✅ WebSocket server closed');
  } catch (error) {
    logger.warn(logCtx({ error }), '⚠️ Error closing WebSocket server');
  }

  try {
    await closeTemporalClient();
    logger.info(logCtx(), '✅ Temporal client closed');
  } catch (error) {
    logger.warn(logCtx({ error }), '⚠️ Error closing Temporal client');
  }

  try {
    await closePool();
    logger.info(logCtx(), '✅ Database pool closed');
  } catch (error) {
    logger.warn(logCtx({ error }), '⚠️ Error closing database pool');
  }

  if (activeListenerClient) {
    try {
      activeListenerClient.removeAllListeners();
      activeListenerClient.release();
      logger.info(logCtx(), '✅ Listener client released');
    } catch (error) {
      logger.warn(logCtx({ error }), '⚠️ Error releasing listener client');
    }
    activeListenerClient = undefined;
  }

  if (natsConnection && !natsConnection.isClosed()) {
    try {
      await natsConnection.close();
      logger.info(logCtx(), '✅ NATS connection closed');
    } catch (error) {
      logger.warn(logCtx({ error }), '⚠️ Error closing NATS connection');
    }
  }

  logger.info(logCtx(), '📊 FIXED architecture shutdown complete');
}

// Process signal handling
process.on('SIGINT', () => shutdownFixedArchitecture('SIGINT').then(() => process.exit(0)));
process.on('SIGTERM', () => shutdownFixedArchitecture('SIGTERM').then(() => process.exit(0)));
process.on('unhandledRejection', (reason, promise) => {
  logger.error(logCtx({ reason, promise }), '🚨 FIXED: Unhandled Rejection');
});
process.on('uncaughtException', (error) => {
  logger.error(logCtx({ error: error.message, stack: error.stack }), '🚨 FIXED: Uncaught Exception');
  process.exit(1);
});

// Bootstrap
import pRetry from 'p-retry';

(async () => {
  logger.info(logCtx(), '🚀 Starting FIXED rail-events-listener bootstrap...');
  logger.info(logCtx({ USE_DAG_RUNNER }), `🚦 DAG_RUNNER = ${USE_DAG_RUNNER}`);
  
  const channels = [
    CHANNEL, 
    'task_ready_for_dag_generation', 
    'intelligent_segmentation_complete', 
    'dag_creation_complete',
    'nats_bridge',
    'worker_status',
    'template_hydrated',
    'task_routed',
    'node_state_change'
  ];
  
  logger.info(logCtx({ 
    channels_count: channels.length,
    channels: channels
  }), `📡 Will subscribe to FIXED channels`);

  try {
    // Bootstrap components
    await pRetry(bootFixedListener, { 
      retries: 5, 
      minTimeout: 1_000, 
      factor: 2,
      onFailedAttempt: (error) => {
        logger.warn(logCtx({ 
          attempt: error.attemptNumber, 
          retriesLeft: error.retriesLeft,
          error: error.message 
        }), '🔄 PostgreSQL connection attempt failed, retrying...');
      }
    });
    logger.info(logCtx(), '✅ FIXED PostgreSQL listener booted');
    
    temporalClient = await getTemporalClient();
    logger.info(logCtx(), '✅ FIXED Temporal client ready');
    
    initializeWebSocketServer(logger);
    logger.info(logCtx(), '✅ FIXED WebSocket server ready');
    
    logger.info(logCtx({
      architecture: 'fixed_clean_separation',
      components: ['routing_worker', 'segmentation_engine', 'template_hydrator'],
      workflow: 'intelligence → segmentation → dag_creation',
      ssl_bypass: true,
      channels_monitored: channels.length
    }), '🎉 FIXED clean architecture application started successfully!');
    
  } catch (err) {
    logger.fatal(logCtx({ 
      error: err instanceof Error ? err.message : String(err),
    }), '💥 Failed to start FIXED listener - exiting');
    await shutdownFixedArchitecture('startup failure');
    process.exit(1);
  }
})();