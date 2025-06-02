export interface BroadcastDelta {
  v: number; // Delta format version
  taskId: string;
  nodeId: number | string;
  state: string;
  eventSubtype?: string | null;
  version: number;      // Monotonic snapshot version (e.g., txid)
  title?: string;         // 👈 NEW
}

export interface RailEvent {
  task_id: string;
  node_id: number | string;
  state: string;
  event_subtype?: string | null;
  generated_title?: string; // 👈 NEW
  [key: string]: any; // Allow other properties from the full event
}

/**
 * Convert a full rail_event row into the minimal delta.
 * `snapshotVersion` must be supplied by the caller (see worker below).
 */
export function toDelta(
  curr: RailEvent,
  prev: RailEvent | null | undefined,
  snapshotVersion: number
): BroadcastDelta {
  return {
    v: 1,
    taskId: curr.task_id,
    nodeId: curr.node_id,
    state:  curr.state,
    version: snapshotVersion,
    title: curr.generated_title, // 👈 NEW
    ...(curr.event_subtype &&
        (!prev || prev.event_subtype !== curr.event_subtype)
        ? { eventSubtype: curr.event_subtype }
        : {})
  };
}
