export default {
    stringValueList: {
        empty: "No values defined.",
    },
    taskConnectionString: {
        label: "Connection String",
        createNew: "Create a new {{type}} connection string",
    },
    taskResponsibleNode: {
        shardedNotRespected: "This option is not respected in case of sharded databases.",
        noNodesAvailable: "Currently, the responsible node cannot be selected because there are no nodes available.",
        setResponsibleNode: "Set Responsible Node",
        nodeOption: "Node {{nodeTag}}",
        pinNodeTitle: "Toggle on to pin selected node",
        pinNode: "Pin node",
        info_pinned:
            "The selected node is now Pinned to handle this task.<br/>When this node is down, the task will Not execute as no other node will be selected to handle the task.<br/>In case the node is removed from the Database Group, a failover will occur as the cluster will select another node to handle the task.",
        info_preferred:
            "The selected node will be the Preferred Node to handle the task.<br/>When this node is down, the cluster selects another node from the Database Group to handle the task.",
    },
} as const;
