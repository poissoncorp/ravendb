export default {
    preferred: {
        selected: "The selected node will be the Preferred Node to handle the task.",
        nodeDown: "When this node is down, the cluster selects another node from the Database Group to handle the task.",
    },
    pinned: {
        selected: "The selected node is now Pinned to handle this task.",
        nodeDown:
            "When this node is down, the task will Not execute as no other node will be selected to handle the task.",
        nodeRemoved:
            "In case the node is removed from the Database Group, a failover will occur as the cluster will select another node to handle the task.",
    },
    shardedNotRespected: "This option won't be respected in case of sharded databases.",
} as const;
