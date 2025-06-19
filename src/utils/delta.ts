// src/utils/delta.ts
export interface BroadcastDelta {
  v: number;                  // Delta version (1)
  taskId: string;
  nodeId: string;             // UUID rendered as plain string
  state: string;
  eventSubtype?: string | null;
  title?: string;             // 🆕 short, AI-generated title
}

export interface RailEvent {
  task_id: string;
  node_id: string;
  state: string;
  event_subtype?: string | null;
  generated_title?: string;   // 🆕 same column we added
  [key: string]: any;         // allow any extra columns
}

/**
 * Pure helper: diff *curr* vs *prev* and emit the lean BroadcastDelta
 */
export function toDelta(
  curr: RailEvent,
  prev: RailEvent | null | undefined,
): BroadcastDelta {
  return {
    v: 1,
    taskId : curr.task_id,
    nodeId : curr.node_id,
    state  : curr.state,
    title  : curr.generated_title ?? undefined,
    // Include eventSubtype if present in the current event
    ...(curr.event_subtype
       ? { eventSubtype: curr.event_subtype }   // ← keep unconditionally
       : {}),
  };
}