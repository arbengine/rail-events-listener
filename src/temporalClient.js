// workers/rail-events-listener/src/temporalClient.ts
import { Connection, WorkflowClient } from '@temporalio/client';
import 'dotenv/config';

let client: WorkflowClient | undefined;

/**
 * Returns a memoized WorkflowClient, trying both apiKey and metadata auth.
 */
export async function getTemporalClient(): Promise<WorkflowClient> {
  if (client) return client;

  // 1. Validate env
  const required = ['TEMPORAL_ADDRESS', 'TEMPORAL_NAMESPACE', 'TEMPORAL_API_KEY'];
  const missing = required.filter(v => !process.env[v]);
  if (missing.length) {
    throw new Error(`Missing Temporal env vars: ${missing.join(', ')}`);
  }

  const { TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY } = process.env;
  console.log(`🔄 Connecting to Temporal at ${TEMPORAL_ADDRESS} (ns: ${TEMPORAL_NAMESPACE})`);

  // Helper to build client once we have a connection
  const makeClient = (conn: Awaited<ReturnType<typeof Connection.connect>>) =>
    new WorkflowClient({ connection: conn, namespace: TEMPORAL_NAMESPACE });

  // 2. Try apiKey-based auth
  try {
    console.log('⏳ Attempting Connection.connect({ apiKey })...');
    const conn = await Connection.connect({
      address: TEMPORAL_ADDRESS,
      tls: {},                 // pick up any mTLS env too
      apiKey: TEMPORAL_API_KEY,
    });
    console.log('✅ Connected to Temporal via apiKey');
    client = makeClient(conn);
    return client;
  } catch (err: any) {
    console.warn('⚠️ apiKey auth failed:', err.message);
  }

  // 3. Fallback to metadata bearer token
  console.log('⏳ Attempting Connection.connect({ metadata auth })...');
  const conn = await Connection.connect({
    address: TEMPORAL_ADDRESS,
    tls: {},
    metadata: { authorization: `Bearer ${TEMPORAL_API_KEY}` },
  });
  console.log('✅ Connected to Temporal via metadata bearer');
  client = makeClient(conn);
  return client;
}
