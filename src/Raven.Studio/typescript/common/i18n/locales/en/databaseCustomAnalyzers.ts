export default {
    title: "Custom analyzers",
    addButton: "Add a custom analyzer",
    addButtonLimitPopover_database:
        "Database has reached the maximum number of Custom Analyzers allowed per database.<br/>Delete unused analyzers or <a>upgrade your license</a>",
    addButtonLimitPopover_cluster:
        "Cluster has reached the maximum number of Custom Analyzers allowed per cluster.<br/>Delete unused analyzers or <a>upgrade your license</a>",
    databaseLimitAlert_limitReached:
        "Database has reached the <strong>maximum number of Custom Analyzers</strong> allowed per database by your license <strong>({{used}}/{{limit}})</strong><br/>Delete unused analyzers or <strong><a>upgrade your license</a></strong>",
    databaseLimitAlert_closeToLimit:
        "Database is reaching the <strong>maximum number of Custom Analyzers</strong> allowed per database by your license <strong>({{used}}/{{limit}})</strong><br/>Delete unused analyzers or <strong><a>upgrade your license</a></strong>",
    clusterLimitAlert_limitReached:
        "Cluster has reached the <strong>maximum number of Custom Analyzers</strong> allowed per cluster by your license <strong>({{used}}/{{limit}})</strong><br/>Delete unused analyzers or <strong><a>upgrade your license</a></strong>",
    clusterLimitAlert_closeToLimit:
        "Cluster is reaching the <strong>maximum number of Custom Analyzers</strong> allowed per cluster by your license <strong>({{used}}/{{limit}})</strong><br/>Delete unused analyzers or <strong><a>upgrade your license</a></strong>",
    databaseHeading: "Database custom analyzers",
    serverWideHeading: "Server-wide custom analyzers",
    serverWideLinkTitle: "Navigate to the server-wide view to edit",
    loadError: "Unable to load custom analyzers",
    empty: "No custom analyzers have been defined",
    serverWideEmpty: "No server-wide custom analyzers have been defined",
    item: {
        newNameFallback: "New analyzer name",
        overridesServerWide: "Override server-wide analyzer",
        nameLabel: "Name",
        namePlaceholder: "Enter analyzer name",
        loadFromFile: "Load from a file",
        saveButton: "Save changes",
        discardButton: "Discard",
    },
    featureAvailability: {
        limitPerDatabase: "Limit per database",
        limitPerCluster: "Limit per cluster",
        serverWide: "Server-wide custom analyzers",
    },
    about: {
        analyzers:
            "<strong>Analyzers</strong> are used by indexes to split the index-fields into tokens (terms).<br/>The analyzer defines how the field is tokenized.<br/>When querying an index, these terms are used to define the search criteria and filter query results.",
        inThisView:
            "<strong>In this view</strong>, you can add your own analyzers in addition to the existing analyzers that come with RavenDB.",
        databaseScopeItem: "The custom analyzers added here can be used only by indexes in this database.",
        serverWideScopeItem: "The server-wide custom analyzers listed can also be used in this database.",
        provideCode: "Provide <code>C#</code> code in the editor view, or upload from file.",
        nameItem: "The analyzer name must be the same as the analyzer's class name in your code.",
        inheritItem: "Inherit from <code>Lucene.Net.Analysis.Analyzer</code>",
        compilableItem: "Code must be compilable and include all necessary <code>using</code> statements.",
        docsLink: "Docs - Custom Analyzers",
    },
} as const;
