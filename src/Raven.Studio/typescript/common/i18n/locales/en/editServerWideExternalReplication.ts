export default {
    licenseRestricted:
        "Your current license does not support this feature.<br /><a href='{{upgradeUrl}}' target='_blank'>Upgrade your plan</a> to access.",
    heading_new: "New Server-Wide External Replication",
    heading_edit: "Edit Server-Wide External Replication",
    cancelTitle: "Return to the Server-Wide Tasks view",
    taskName: "Task Name",
    taskNamePlaceholder: "Enter a name for the server-wide replication task (optional)",
    taskState: "Task State",
    stateEnabled: "Enabled",
    stateDisabled: "Disabled",
    delayTimeTitle: "Set a delay time for the task",
    setDelayTime: "Set Replication Delay Time",
    delayTime: "Delay Time",
    responsibleNodeTitle: "Toggle on to set a responsible node for the task",
    setResponsibleNode: "Set responsible node",
    excludeDatabases: {
        title: "Select databases to exclude from the server-wide external replication",
        label: "Exclude databases",
        placeholder: "Select (or enter) a database to exclude",
        add: "Add Database",
        excludedHeading: "Excluded Databases:",
        removeTitle: "Remove database",
    },
    connectionString: {
        label: "Connection String",
        createdPerDatabase: "A connection string will be automatically created per database.",
        sourceNameAsDestination: "The source database name will be used as the destination database.",
        discoveryUrls: "Discovery URLs",
        discoveryUrlPlaceholder: "Enter discovery url to add",
        addUrl: "Add URL",
        removeUrlTitle: "Remove url",
        testUrlTitle: "Test url connection",
    },
    testConnection: {
        success: "Successfully connected to node",
        failed: "Connection test failed!",
        details_hide: "hide details",
        details_show: "show details",
        aboutError: "About this error",
        endpoints:
            "Each RavenDB server has both HTTP and TCP endpoints. While the first one is used for system management and client-server rest request, the second is used for inter-server and advanced client-server communications.",
        tcpAfterHttp: "The connection tests the TCP endpoint only after a successful HTTP connection.",
        httpSucceeded:
            "It appears that the current server was able to connect to the desired server through HTTP, but failed connecting to it using TCP.",
        httpFailed: "It appears that the current server could not connect to the desired node through HTTP.",
        pleaseVerify: "Please verify that:",
        urlCorrect: "The URL is correct",
        machinesRunning: "Both RavenDB and the target machine are up and running",
        noFirewall: "There are no firewall settings on either machine blocking usage of that URL",
        noNetworkConfig: "There are no network configurations that prevent communication",
    },
    about: {
        serverWideTask:
            "Defining a <strong>Server-Wide External-Replication task</strong> will create an ongoing External-Replication task for each database in your cluster.",
        excludeItem: "You can select specific databases to exclude from the task.",
        configurationsItem:
            "The configurations set in the Server-Wide task will be applied to the corresponding ongoing task created per database.",
        connectionString:
            "The <strong>connection string</strong> used in each corresponding database task will be composed of:",
        discoveryUrlsItem: "The discovery URLs provided here.",
        databaseNameItem: "The target database name will be the individual source database name.",
    },
    featureAvailability: {
        serverWideExternalReplication: "Server-Wide External Replication",
    },
} as const;
