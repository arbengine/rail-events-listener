// workers/rail-events-listener/src/temporalClient.ts
import { Connection, WorkflowClient } from '@temporalio/client';
import 'dotenv/config';
import pino from 'pino';

// Initialize pino logger for this module, using 'as any' to align with index.ts and bypass call signature issues
const pinoInstance = pino as any; 
const temporalLogger = pinoInstance({ name: 'temporal-client-ts', level: process.env.LOG_LEVEL || 'info' });

let workflowClientInstance: WorkflowClient | undefined;

/**
 * Returns a memoized WorkflowClient, trying both apiKey and metadata auth.
 */
export async function getTemporalClient(): Promise<WorkflowClient> {
  if (workflowClientInstance) return workflowClientInstance;

  // 1. Validate env
  const required = ['TEMPORAL_ADDRESS', 'TEMPORAL_NAMESPACE', 'TEMPORAL_API_KEY'];
  const missing = required.filter(v => !process.env[v]);
  if (missing.length) {
    const errorMsg = `Missing Temporal env vars: ${missing.join(', ')}`;
    temporalLogger.error(errorMsg);
    throw new Error(errorMsg);
  }

  const { TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY } = process.env;
  temporalLogger.info(`🔄 Connecting to Temporal at ${TEMPORAL_ADDRESS} (ns: ${TEMPORAL_NAMESPACE})`);

  // Helper to build client once we have a connection
  const makeClient = (conn: Connection) =>
    new WorkflowClient({ connection: conn, namespace: TEMPORAL_NAMESPACE });

  let connection: Connection | undefined;

  // 2. Try apiKey-based auth
  try {
    temporalLogger.info('⏳ Attempting Connection.connect({ apiKey })...');
    connection = await Connection.connect({
      address: TEMPORAL_ADDRESS,
      tls: {},                 // pick up any mTLS env too
      apiKey: TEMPORAL_API_KEY,
    });
    temporalLogger.info('✅ Connected to Temporal via apiKey');
    workflowClientInstance = makeClient(connection);
    return workflowClientInstance;
  } catch (err: any) {
    temporalLogger.warn({ err, message: err.message }, '⚠️ apiKey auth failed for Temporal');
  }

  // 3. Fallback to metadata bearer token
  temporalLogger.info('⏳ Attempting Connection.connect({ metadata auth })...');
  try {
    connection = await Connection.connect({
      address: TEMPORAL_ADDRESS,
      tls: {},
      metadata: { authorization: `Bearer ${TEMPORAL_API_KEY}` },
    });
    temporalLogger.info('✅ Connected to Temporal via metadata bearer');
    workflowClientInstance = makeClient(connection);
    return workflowClientInstance;
  } catch (err: any) {
    temporalLogger.error({ err, message: err.message }, '⚠️ Metadata bearer auth failed for Temporal. All connection attempts failed.');
    throw err;
  }
}

/**
 * Closes the Temporal client connection if it's active.
 */
export async function closeTemporalClient(): Promise<void> {
  if (workflowClientInstance && workflowClientInstance.connection) {
    temporalLogger.info('⏳ Closing Temporal client connection...');
    try {
      await workflowClientInstance.connection.close();
      temporalLogger.info('✅ Temporal client connection closed.');
    } catch (err: any) {
      temporalLogger.error({ err, message: err.message }, '⚠️ Error closing Temporal client connection.');
    } finally {
      workflowClientInstance = undefined;
    }
  } else {
    temporalLogger.info('ℹ️ No active Temporal client/connection to close.');
  }
}
