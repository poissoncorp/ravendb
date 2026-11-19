export default {
    addTitle: "Saves the new cluster node",
    add: "Add",
    form: {
        urlPlaceholder: "Enter server url",
        tag: "Tag",
        tagPlaceholder: "Manually specify the node tag (optional)",
        addAsWatcher: "Add node as Watcher",
        watcherInfo:
            "A Watcher is a non-voting node in the cluster that is still fully managed by the cluster. <br />A Watcher can be assigned databases and work to be done.",
        useAvailableCores: "Use available cores",
        maxCores: "Maximum cores to use",
        maxCoresPlaceholder: "Enter the maximum number of cores to be used by the new node",
    },
    test: {
        title: "Test connection to newly created node",
        button: "Test connection",
        success: "Successfully connected to node",
        failed: "Connection test failed!",
        hideDetails: "hide details",
        showDetails: "show details",
    },
    about: {
        title: "About this error",
        endpoints:
            "Each RavenDB server has both HTTP and TCP endpoints. While the first one is used for system management and client-server rest request, the second is used for inter-server and advanced client-server communications.",
        tcpAfterHttp: "The connection tests the TCP endpoint only after a successful HTTP connection.",
        tcpFailed:
            "It appears that the current server was able to connect to the desired server through HTTP, but failed connecting to it using TCP.",
        httpFailed: "It appears that the current server could not connect to the desired node through HTTP.",
        verify: "Please verify that:",
        urlCorrect: "The URL is correct",
        upAndRunning: "Both RavenDB and the target machine are up and running",
        noFirewall: "There is no firewall settings on either machine blocking usage of that URL",
        noNetworkConfig: "There is no network configurations that prevent communication",
    },
} as const;
