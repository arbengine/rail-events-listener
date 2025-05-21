// workers/rail-events-listener/src/temporalClient.ts
import {
    Connection,
    WorkflowClient,
    type WorkflowClientOptions,
  } from '@temporalio/client';
  
  let client: WorkflowClient | undefined;
  
  /**
   * Returns a memoised WorkflowClient that other modules can share.
   * Honors standard Temporal env vars:
   *   • TEMPORAL_ADDRESS          (host:port)
   *   • TEMPORAL_NAMESPACE        (defaults to 'default')
   *   • TEMPORAL_MTLS_ENABLED     etc. (SDK v3 picks these up)
   */
  export async function getTemporalClient(
    opts: WorkflowClientOptions = {},
  ): Promise<WorkflowClient> {
    if (client) return client;
  
    // Will read TLS / mTLS env automatically
    const connection = await Connection.connect();
    client = new WorkflowClient({ connection, ...opts });
  
    return client;
  }
  