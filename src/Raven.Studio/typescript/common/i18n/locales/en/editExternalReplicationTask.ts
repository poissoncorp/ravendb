export default {
    licenseRestricted:
        "Your current license does not support this feature.<br /><a href='{{upgradeUrl}}' target='_blank'>Upgrade your plan</a> to access.",
    heading_new: "New External Replication",
    heading_edit: "Edit External Replication",
    cancelTitle: "Return to Ongoing Tasks View",
    taskName: "Task Name",
    taskNamePlaceholder: "Enter a descriptive name for the replication task (optional)",
    taskState: "Task State",
    stateEnabled: "Enabled",
    stateDisabled: "Disabled",
    delayTimeTitle: "Toggle on to set a delay time for the task",
    setDelayTime: "Set Replication Delay Time",
    delayTime: "Delay Time",
    responsibleNodeTitle: "Toggle on to set a responsible node for the task",
    responsibleNodeNotSupportedSharded: "Not supported in sharded databases",
    setResponsibleNode: "Set responsible node",
    createNewConnectionString: "Create a new RavenDB connection string",
    connectionString: {
        label: "Connection String",
        noneDefinedTitle: "No connection strings were defined",
        select: "Select a connection string",
        destinationDb: "Destination DB:",
        noneDefinedYet: "No connection strings have been defined yet",
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
        task: "Schedule an <strong>External Replication</strong> ongoing-task in order to have a live replica of your data in another RavenDB database in another cluster.",
        failoverItem: "The replica can serve as a failover solution in case the source cluster is down.",
        whatIsReplicated: "What is replicated:",
        documentsItem:
            "Documents and their related data (attachments, revisions, counters, time series) will be replicated.",
        clusterItemsItem:
            "Server and cluster-level items (e.g. indexes, identities, compare-exchange items, ongoing tasks definitions, etc.) are Not replicated.",
        taskDefinition: "Task definition includes:",
        connectionStringItem:
            "A connection string to the destination database containing the URLs of the target cluster's servers.",
        delayItem: "An optional delay time for data replication.",
        responsibleNodeItem: "A responsible node to handle this task can be set.",
        docsLink: "Docs - External Replication",
    },
    featureAvailability: {
        externalReplication: "External Replication",
    },
} as const;
