// workers/rail-events-listener/src/temporalClient.ts
import { Connection, WorkflowClient } from '@temporalio/client';
import 'dotenv/config';

let workflowClientInstance: WorkflowClient | undefined;

/**
 * Returns a memoized WorkflowClient, trying both apiKey and metadata auth.
 */
export async function getTemporalClient(): Promise<WorkflowClient> {
  if (workflowClientInstance) return workflowClientInstance;

  // 1. Validate env
  const requiredVars = ['TEMPORAL_ADDRESS', 'TEMPORAL_NAMESPACE', 'TEMPORAL_API_KEY'];
  const missingVars = requiredVars.filter(v => !process.env[v]);
  if (missingVars.length > 0) {
    const errorMsg = `Missing required environment variables: ${missingVars.join(', ')}`;
    console.error(errorMsg);
    throw new Error(errorMsg);
  }

  const { TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY } = process.env;
  console.log(`🔄 Connecting to Temporal with address: ${TEMPORAL_ADDRESS}`);
  console.log(`🔑 API Key exists: ${Boolean(TEMPORAL_API_KEY)}, Length: ${TEMPORAL_API_KEY?.length || 0}`);

  // Helper to build client once we have a connection
  const makeClient = (conn: Connection) =>
    new WorkflowClient({ connection: conn, namespace: TEMPORAL_NAMESPACE! });

  let connection: Connection | undefined;

  // 2. Try apiKey-based auth
  try {
    console.log('Attempting connection with apiKey parameter...');
    connection = await Connection.connect({
      address: TEMPORAL_ADDRESS!,
      tls: {}, 
      apiKey: TEMPORAL_API_KEY!,
    });
    console.log('Connection successful with apiKey parameter');
    workflowClientInstance = makeClient(connection);
    return workflowClientInstance;
  } catch (err: any) {
    console.error('Connection failed with apiKey parameter:', err.message);
  }

  // 3. Fallback to metadata bearer token
  console.log('Attempting connection with explicit authorization metadata...');
  try {
    connection = await Connection.connect({
      address: TEMPORAL_ADDRESS!,
      tls: {},
      metadata: { authorization: `Bearer ${TEMPORAL_API_KEY}` },
    });
    console.log('Connection successful with metadata approach');
    workflowClientInstance = makeClient(connection);
    return workflowClientInstance;
  } catch (err: any) {
    console.error('Connection failed with metadata approach. All connection attempts failed:', err.message);
    throw err;
  }
}

/**
 * Closes the Temporal client connection if it's active.
 */
export async function closeTemporalClient(): Promise<void> {
  if (workflowClientInstance && workflowClientInstance.connection) {
    console.log('⏳ Closing Temporal client connection...');
    try {
      await workflowClientInstance.connection.close();
      console.log('✅ Temporal client connection closed.');
    } catch (err: any) {
      console.error('⚠️ Error closing Temporal client connection:', err.message);
    } finally {
      workflowClientInstance = undefined;
    }
  } else {
    console.log('ℹ️ No active Temporal client/connection to close.');
  }
}
