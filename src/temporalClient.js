// src/temporalClient.js
import { Connection, WorkflowClient } from '@temporalio/client';

let client;

/**
 * Returns a singleton Temporal WorkflowClient.
 * Honors standard Temporal env vars for connection options.
 */
export async function getTemporal(opts = {}) {
  if (client) return client;

  // Connection.connect() will automatically pick up env vars like
  // TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_MTLS_CERT etc.
  const connection = await Connection.connect(); 
  client = new WorkflowClient({ connection, ...opts }); 
  return client;
}