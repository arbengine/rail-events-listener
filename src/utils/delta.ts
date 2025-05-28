export interface RailEvent {
  task_id: string;
  node_id: number | string;
  state: string;
  event_subtype?: string | null;
  [key: string]: any; // Allow other properties from the full event
}

export function toDelta(prev: RailEvent | null, curr: RailEvent) {
  return {
    v: 1,
    taskId: curr.task_id,
    nodeId: curr.node_id,
    state: curr.state,
    ...(curr.event_subtype &&
        (!prev || prev.event_subtype !== curr.event_subtype)
        ? { eventSubtype: curr.event_subtype }
        : {})
  };
}
