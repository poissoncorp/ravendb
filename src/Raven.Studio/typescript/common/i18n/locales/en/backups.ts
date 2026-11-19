export default {
    restoreDatabase: {
        title: "Navigate to creating a new database from a backup",
        button: "Restore a database from a backup",
    },
    about: {
        backups:
            "<strong>Backups</strong> save your data at a specific point in time and allow you to restore your database to that point.",
        viewEnablesCreating: "This view enables creating:",
        manualBackup: "<strong>Manual Backup</strong><br/>Create a one-time backup for this database.",
        periodicBackups:
            "<strong>Periodic Backups</strong><br/>Define an ongoing-task that will automatically create periodic backups for this database at the defined schedule.",
        docsLink: "Docs - Backups",
    },
    manualBackup: {
        heading: "Manual Backup",
        createTitle: "Backup the database now",
        create: "Create a Backup",
        loadError: "Unable to load data: {{message}}",
        empty: "No manual backup created",
        neverBackedUp: "Never backed up",
        recentBackup: "Recent Backup",
        type: "Type",
        backupType_Backup: "Backup",
        backupType_Snapshot: "Snapshot",
        destinations: "Destinations",
        destinationLocal: "Local",
        noDestinations: "No destinations defined",
        encrypted: "Backup is encrypted",
        notEncrypted: "Backup is not encrypted",
        nodeTitle: "Cluster node that created this backup",
    },
    periodicBackup: {
        serverWideTasksTitle: "Navigate to the Server-Wide Tasks View",
        serverWideTasksLink: "Go to Server-Wide Tasks View",
        heading_one: "Periodic Backup ({{count}})",
        heading_other: "Periodic Backup ({{count}})",
        createTitle: "Create an ongoing periodic backup task",
        create: "Create a Periodic Backup",
        empty: "No periodic backup tasks created",
    },
} as const;
