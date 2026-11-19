export default {
    title: "Server-Wide Tasks",
    loadError: "Unable to load server-wide tasks",
    addButton: "Add a Server-Wide Task",
    empty: "No server-wide tasks have been created",
    perDatabaseLink: {
        label: "Per-database ongoing tasks",
        title: "Go to the Ongoing Tasks view of the selected database",
        noDatabase: "Select a database to go to its per-database ongoing tasks view",
    },
    filter: {
        nameLabel: "Filter by name",
        namePlaceholder: "e.g. BackupTask",
        nameTitle: "Filter server-wide tasks",
        typeLabel: "Filter by type",
        noMatch: "No tasks match your filter criteria",
    },
    taskType: {
        replication: "External replication",
        backup: "Backup",
    },
    task: {
        nameTitle: "Task name: {{name}}",
        responsibleNodeTitle: "Cluster node that is responsible for this task",
        stateTitle: "Set task state",
        state_Enabled: "Enabled",
        state_Disabled: "Disabled",
        state_PartiallyEnabled: "PartiallyEnabled",
        state_None: "None",
        enable: "Enable",
        disable: "Disable",
        detailsTitle: "Click for details",
        editTitle: "Edit task",
        deleteTitle: "Delete task",
        excludedDatabases: "Excluded databases",
    },
    replication: {
        discoveryUrls: "Topology discovery URLs",
        noUrls: "No URLs defined",
        delay: "Replication delay",
    },
    backup: {
        backupType: "Backup type",
        destinations: "Destinations",
        noDestinations: "No destinations defined",
        encrypted: "Encrypted",
        yes: "Yes",
        no: "No",
        retention: "Retention",
    },
    operations: {
        toggleSuccess_enabled_one: "Task enabled successfully.",
        toggleSuccess_enabled_other: "Tasks enabled successfully.",
        toggleSuccess_disabled_one: "Task disabled successfully.",
        toggleSuccess_disabled_other: "Tasks disabled successfully.",
        deleteSuccess_one: "Task deleted successfully.",
        deleteSuccess_other: "Tasks deleted successfully.",
    },
    add: {
        title: "Add a Server-Wide Task",
        replicationTitle: "Server-wide External Replication",
        replicationDescription:
            "Create live replicas of all databases in your cluster, replicating each to a corresponding RavenDB database with the same name in another cluster.",
        backupTitle: "Server-wide Backup",
        backupDescription:
            "Create periodic backups or snapshots of all databases in your cluster, managing schedule, retention, and destination from a single task.",
    },
    about: {
        heading: "About this view",
        description: "Get additional info on this feature",
        intro: "<strong>Server-Wide Tasks</strong> are ongoing tasks defined once at the cluster level and applied to all databases in the cluster.",
        backupItem:
            "<strong>Server-wide Backup</strong> creates periodic backups or snapshots of all databases in your cluster.",
        replicationItem:
            "<strong>Server-wide External Replication</strong> creates live replicas of all databases in another cluster.",
        exclusion: "Specific databases can be excluded from each task.",
    },
} as const;
