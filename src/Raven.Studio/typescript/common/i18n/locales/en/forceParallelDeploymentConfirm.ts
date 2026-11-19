export default {
    title: "Select force action",
    allNodes: {
        heading: "Parallel Indexing - All Nodes",
        description: "Indexing deployment will be continued in Parallel mode on all nodes.",
        forceButton: "Force Parallel - All",
    },
    currentNode: {
        heading: "Parallel Indexing - Current Node Only",
        description: "Indexing deployment on this node only will continue in parallel to all other nodes.",
        otherNodesDescription: "Indexing deployment on all other nodes will be continued in Rolling mode.",
        forceButton: "Force Parallel - Current",
    },
    close: "Close",
} as const;
