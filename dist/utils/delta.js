/**
 * Pure helper: diff *curr* vs *prev* and emit the lean BroadcastDelta
 * The caller (rail-events listener) supplies the monotonic snapshotVersion.
 */
export function toDelta(curr, prev, snapshotVersion) {
    return {
        v: 1,
        taskId: curr.task_id,
        nodeId: curr.node_id,
        state: curr.state,
        version: snapshotVersion,
        title: curr.generated_title ?? undefined,
        // Include eventSubtype if present in the current event
        ...(curr.event_subtype
            ? { eventSubtype: curr.event_subtype } // ← keep unconditionally
            : {}),
    };
}
