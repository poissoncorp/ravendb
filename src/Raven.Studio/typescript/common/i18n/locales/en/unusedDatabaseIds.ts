export default {
    title: "Unused Database IDs",
    copied: "Copied {{id}} vector to clipboard",
    usedIds: {
        heading: "Used IDs",
        copyTitle: "Copy ID",
        nodeTitle: "Node {{nodeTag}}",
        shardTitle: "Shard {{shardNumber}}",
    },
    unusedIds: {
        heading: "Unused IDs",
        removeAll: "Remove all",
        removeAllTitle: "Remove all unused IDs from the list",
        info: "The Unused Database IDs will not be used in the Change Vector generated for a new or modified document",
        placeholder: "Enter database ID to add",
        addButton: "Add ID",
        addButtonTitle: "Add ID to the list",
        empty: "No Unused IDs have been added",
        copyTitle: "Copy to clipboard",
        removeTitle: "Remove",
    },
    potentialIds: {
        heading: "IDs that may be added to the list",
        addAll: "Add all",
        addAllTitle: "Add all potential unused IDs to the list",
        alreadyAdded: "This ID has already been added to the list",
        copyTitle: "Copy ID",
    },
    about: {
        usedIdsItem:
            'Each database instance in the cluster has its own database ID (listed under "<strong>Used IDs</strong>").<br/> These IDs are used to construct the change-vector generated for each document that is created or modified within the database.',
        scenariosItem:
            "In some scenarios, such as restoring a database from a Snapshot, or when receiving data from a source database via an External-Replication task, the change-vectors of incoming documents, which include the source database IDs, are added to the change-vector of the target database.",
        unusedIdsItem:
            'To avoid using these database IDs which are no longer relevant in the current database,<br/> add them to the list of "<strong>Unused IDs</strong>" so they will not be utilized when creating new change-vectors.',
        noteItem:
            "Note:<br/> Do not add database IDs that are currently in use by other nodes in the database-group to this list.",
    },
} as const;
