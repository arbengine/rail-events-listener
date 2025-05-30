/**
 * Convert a full rail_event row into the minimal delta.
 * `snapshotVersion` must be supplied by the caller (see worker below).
 */
export function toDelta(curr, prev, snapshotVersion) {
    return {
        v: 1,
        taskId: curr.task_id,
        nodeId: curr.node_id,
        state: curr.state,
        version: snapshotVersion,
        ...(curr.event_subtype &&
            (!prev || prev.event_subtype !== curr.event_subtype)
            ? { eventSubtype: curr.event_subtype }
            : {})
    };
}
