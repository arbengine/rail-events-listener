// workers/rail-events-listener/src/temporalClient.ts
import { Connection, WorkflowClient, } from '@temporalio/client';
let client;
/**
 * Returns a memoised WorkflowClient that other modules can share.
 * Honors standard Temporal env vars:
 *   • TEMPORAL_ADDRESS          (host:port)
 *   • TEMPORAL_NAMESPACE        (defaults to 'default')
 *   • TEMPORAL_MTLS_ENABLED     etc. (SDK v3 picks these up)
 */
export async function getTemporalClient(opts = {}) {
    if (client)
        return client;
    // Will read TLS / mTLS env automatically
    const connection = await Connection.connect();
    client = new WorkflowClient({ connection, ...opts });
    return client;
}
