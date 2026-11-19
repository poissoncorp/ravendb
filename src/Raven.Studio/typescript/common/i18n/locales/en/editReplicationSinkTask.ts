export default {
    heading_new: "New Replication Sink",
    heading_edit: "Edit Replication Sink",
    licenseRestricted:
        "Your current license does not support this feature.<br /><a href='{{upgradeUrl}}' target='_blank'>Upgrade your plan</a> to access.",
    cancelTitle: "Return to Ongoing Tasks View",
    importConfigurationTitle: "Import configuration from the configuration file created on the Hub",
    importConfiguration: "Import configuration from Hub",
    importConfigurationInvalid: "Invalid configuration format",
    importConfigurationParseFailed: "Can't parse configuration",
    task: {
        nameLabel: "Sink Task Name",
        namePlaceholder: "Enter a descriptive name for the Sink task (optional)",
        hubNameLabel: "Hub Task Name",
        hubNamePlaceholder: "Enter the Replication Hub task name",
        stateLabel: "Task State",
        stateEnabled: "Enabled",
        stateDisabled: "Disabled",
        responsibleNodeShardedTitle: "Not supported in sharded databases",
        responsibleNodeTitle: "Toggle on to set a responsible node for the task",
        responsibleNodeToggle: "Set responsible node",
        allowHubToSink: "Allow replication from Hub to Sink",
        allowSinkToHub: "Allow replication from Sink to Hub",
    },
    connectionString: {
        createNew: "Create a new RavenDB connection string",
        label: "Connection String",
        noneDefinedTitle: "No connection strings were defined",
        select: "Select a connection string",
        destinationDb: "Destination DB:",
        noneDefinedYet: "No connection strings have been defined yet",
    },
    testConnection: {
        success: "Successfully connected to node",
        failed: "Connection test failed!",
        hideDetails: "hide details",
        showDetails: "show details",
        aboutError: "About this error",
        endpoints:
            "Each RavenDB server has both HTTP and TCP endpoints. While the first one is used for system management and client-server rest request, the second is used for inter-server and advanced client-server communications.",
        tcpAfterHttp: "The connection tests the TCP endpoint only after a successful HTTP connection.",
        tcpFailed:
            "It appears that the current server was able to connect to the desired server through HTTP, but failed connecting to it using TCP.",
        httpFailed: "It appears that the current server could not connect to the desired node through HTTP.",
        verify: "Please verify that:",
        urlCorrect: "The URL is correct",
        serversRunning: "Both RavenDB and the target machine are up and running",
        noFirewall: "There are no firewall settings on either machine blocking usage of that URL",
        noNetworkIssues: "There are no network configurations that prevent communication",
    },
    access: {
        heading: "Replication Access",
        nameLabel: "Access Name",
        namePlaceholder: "Enter a name for the Replication Access",
    },
    certificate: {
        sourceLabel: "Certificate Source",
        sourceTitle: "Select certificate source",
        provideOwn: "Provide your own certificate",
        useServer: "Use the server certificate",
        info: {
            authenticate: "The certificate is used to authenticate the Sink when connecting to the Hub.",
            keys: "The Sink keeps both the <strong>public & private keys</strong>.",
            matchingKey: "The Hub task must contain a matching public key.",
        },
        download: "Download",
        downloadInfo: {
            serverCertificates: "Download the cluster's server certificate(s) as a *.pfx file.",
            publicKeysOnly: "Only public keys are downloaded.",
        },
        fileLabel: "Certificate File",
        filePlaceholder: "Upload *.pfx, or import configuration from Hub",
        browse: "Browse",
        passphraseLabel: "Certificate Passphrase",
        label: "Certificate",
        thumbprintTitle: "Certificate thumbprint",
        removeTitle: "Remove certificate",
        validFrom: "Valid From",
        expiration: "Expiration",
    },
    paths: {
        heading: "Filter Replicated Documents by ID Paths",
        hubToSinkLabel: "From Hub to Sink",
        hubToSinkInfo: {
            allowed: "These ID paths define <strong>what documents the Sink allows the Hub to send</strong>.",
            hubDefinition:
                "The documents will be sent from the Hub only if these paths are also allowed on the Hub task definition.",
        },
        placeholder: "Enter documents ID path. Use <path>/* for all child paths.",
        addPath: "Add Path",
        removeTitle: "Remove prefix path",
        useForBothDirections: "Use above paths (<i>Hub to Sink</i>) for both directions",
        sinkToHubLabel: "From Sink to Hub",
        sinkToHubInfo: {
            allowed:
                "These ID paths define <strong>what documents are allowed to be sent from this Sink to the Hub</strong>.",
            hubDefinition:
                "The documents will be sent to the Hub only if these paths are also allowed on the Hub task definition.",
        },
        allChildPathsInfo:
            "To send all documents under some path use <code>&lt;path&gt;/*</code> or <code>&lt;path&gt;-*</code>",
        sameAsHubToSink: "Same as <i>Hub to Sink</i>",
    },
    about: {
        definition:
            "Define a <strong>Replication Sink</strong> task for opening a communication channel between this database and a central Replication Hub that is located in another RavenDB cluster.",
        initiatedBySink: "The connection to the Hub is always initiated by the Sink.",
        multipleSinks: "Multiple Sinks can connect to the same Replication Hub.",
        bidirectional: "Data replication can be bi-directional.",
        replicatedData:
            "Only documents and their related data (attachments, revisions, counters & time series) will be replicated.",
        secureServer: "When running with a SECURE server:",
        replicationAccess:
            "<strong>Replication Access</strong>:<br/>Set up a Replication Access definition on the Sink task with a certificate that will be used by the Sink task to authenticate and establish a connection with the Replication Hub.",
        filteredReplication:
            "<strong>Filtered Replication</strong>:<br/>Within the Access definition, you can filter the documents that will be replicated by their ID paths.",
        sinkToHub: "<strong>Sink to Hub Replication</strong> is only available with a secure server.",
        taskDefinition: "Task definition includes:",
        connectionString: "A connection string to the Replication Hub.",
        direction: "Setting the replication direction.",
        accessDefinition: "The Replication Access definition.",
        responsibleNode: "A responsible node to handle this task can be set.",
        docsLink: "Docs - Replication Sink",
    },
    featureAvailability: {
        replicationSink: "Replication Sink",
    },
} as const;
