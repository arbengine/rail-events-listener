// src/temporalClient.d.ts
import type { WorkflowClient, WorkflowClientOptions } from '@temporalio/client';

declare module './temporalClient.js' {
  export function getTemporal(opts?: WorkflowClientOptions): Promise<WorkflowClient>;
}
