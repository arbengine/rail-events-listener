export function toDelta(prev, curr) {
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
