export default {
    title: "Server-Wide Client Configuration",
    loadError: "Unable to load client global configuration",
    identityPartsSeparator: {
        label: "Identity parts separator",
        info: "Set the default separator for automatically generated document IDs (<i>Identity</i>, <i>HiLo</i>, and <i>Server-side</i>).<br/>Use any character except <code>'|'</code> (pipe).",
        placeholder: "'/' (default)",
    },
    maximumNumberOfRequests: {
        label: "Maximum number of requests per session",
        info: "Set this number to restrict the number of requests (<code>Reads</code> & <code>Writes</code>) per session in the client API.",
        placeholder: "30 (default)",
    },
    loadBalancing: {
        heading: "Load Balancing Client Requests",
        tutorialTitle: "Navigate to the documentation",
        tutorialLink: "Load balancing tutorial",
    },
    loadBalanceBehavior: {
        label: "Load Balance Behavior",
        info: "Set the Load balance method for <strong>Read</strong> & <strong>Write</strong> requests.",
        noneItem:
            "<code>None</code><br/><strong>Read</strong> requests - the node the client will target will be based on Read balance behavior configuration.<br/><strong>Write</strong> requests - will be sent to the preferred node.",
        sessionContextItem:
            "<code>Use session context</code><br/>Sessions that are assigned the same context will have all their <strong>Read & Write</strong> requests routed to the same node.<br/>The session context is hashed from a context string (given by the client) and an optional seed.",
    },
    seed: {
        label: "Seed",
        info: "An optional seed number.<br/>Used when hashing the session context.",
        placeholder: "0 (default)",
    },
    readBalanceBehavior: {
        label: "Read Balance Behavior",
        info: "Set the Read balance method the client will use when accessing a node with <code>Read</code> requests.<br/><code>Write</code> requests are sent to the preferred node.",
    },
    about: {
        thisView:
            "This is the <strong>Server-wide Client-Configuration</strong> view.<br/>The available Client-Configuration options will apply to any client that communicates with any database in the cluster.",
        perDatabase:
            "These values can be customized per database in the <strong>Database Client-Configuration</strong> view.",
        override:
            "Setting the Client-Configuration on the server from this view will <strong>override</strong> the client's existing settings, which were initially set by your client code.",
        nextRequest:
            "When the server's Client-Configuration is modified, the running client will receive the updated settings the next time it makes a request to the server.",
        dynamicControl:
            "This enables administrators to <strong>dynamically control the client behavior</strong> even after it has started running. E.g. manage load balancing of client requests on the fly in response to changing system demands.",
        docsLink: "Docs - Client Configuration",
    },
    featureAvailability: {
        clientConfiguration: "Client Configuration",
    },
} as const;
