// src/temporalClient.d.ts
import type { WorkflowClient } from '@temporalio/client';

declare module './temporalClient.js' { 
  export function getTemporalClient(): Promise<WorkflowClient>;
  export function closeTemporalClient(): Promise<void>;
}
