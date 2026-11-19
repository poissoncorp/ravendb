export default {
    title: "Client Configuration",
    loadError: "Unable to load client configuration",
    serverWideLinkTitle: "Navigate to the server-wide Client Configuration View",
    serverWideLink: "Go to Server-Wide Client Configuration View",
    useServerConfig: "Use server config",
    useDatabaseConfig: "Use database config",
    serverConfigHeading: "Server Configuration",
    serverSettingsTitle: "Server settings",
    databaseConfigHeading: "Database Configuration",
    identityPartsSeparator: {
        label: "Identity parts separator",
        info: "Set the default separator for automatically generated document IDs (<i>Identity</i>, <i>HiLo</i>, and <i>Server-side</i>).<br/>Use any character except <code>'|'</code> (pipe).",
        placeholder: "'/' (default)",
    },
    maxRequests: {
        label: "Maximum number of requests per session",
        serverInfo:
            "Set this number to restrict the number of requests (<code>Reads</code> & <code>Writes</code>) per session in the client API.",
        databaseInfo:
            "Set this number to restrict the number of requests<br/>(<code>Reads</code> & <code>Writes</code>) per session in the client API.",
        placeholder: "30 (default)",
    },
    loadBalancing: {
        heading: "Load Balancing Client Requests",
        tutorialLinkTitle: "Navigate to the documentation",
        tutorialLink: "Load balancing tutorial",
        behaviorLabel: "Load Balance Behavior",
        behaviorInfo: "Set the Load balance method for <strong>Read</strong> & <strong>Write</strong> requests.",
        behaviorInfoNone:
            "<code>None</code><br/><strong>Read</strong> requests - the node the client will target will be based on Read balance behavior configuration.<br/><strong>Write</strong> requests - will be sent to the preferred node.",
        behaviorInfoSessionContext:
            "<code>Use session context</code><br/>Sessions that are assigned the same context will have all their <strong>Read & Write</strong> requests routed to the same node.<br/>The session context is hashed from a context string (given by the client) and an optional seed.",
        nonePlaceholder: "None",
        seedLabel: "Seed",
        seedInfo: "An optional seed number.<br/>Used when hashing the session context.",
        seedPlaceholder: "0 (default)",
        readBehaviorLabel: "Read Balance Behavior",
        readBehaviorInfo:
            "Set the Read balance method the client will use when accessing a node with <code>Read</code> requests.<br/><code>Write</code> requests are sent to the preferred node.",
    },
    about: {
        viewItem:
            "This is the <strong>Database Client-Configuration</strong> view.<br/>The values set in this view will apply to any client communicating with this database.",
        overrideServerWideItem:
            "If the <strong>Server-wide Client-Configuration</strong> view has any values set,<br/> then this view provides the option to override the Server-wide Client-Configuration<br/> and customize specific values for this database.",
        overrideClientItem:
            "Setting the Client-Configuration on the server from this view will <strong>override</strong> the client's existing settings, which were initially set by your client code.",
        updateItem:
            "When the server's Client-Configuration is modified, the running client will receive the updated settings the next time it makes a request to the server.",
        dynamicControlItem:
            "This enables administrators to <strong>dynamically control the client behavior</strong> even after it has started running. E.g. manage load balancing of client requests on the fly in response to changing system demands.",
        docsLink: "Docs - Client Configuration",
    },
    featureAvailability: {
        clientConfiguration: "Client Configuration",
    },
} as const;
