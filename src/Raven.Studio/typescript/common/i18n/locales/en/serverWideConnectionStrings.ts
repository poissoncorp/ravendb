export default {
    title: "Server-Wide Connection Strings",
    loadError: "Unable to load server-wide connection strings",
    addButton: "Add a server-wide connection string",
    empty: "No server-wide connection strings have been defined",
    about: {
        heading: "About this view",
        description: "Get additional info on this feature",
        manageItem: "Use this view to manage server-wide connection strings at the cluster level.",
        propagationItem:
            "<strong>Server-wide connection strings</strong> are automatically propagated to ALL databases in the cluster, unless specific databases are excluded.",
        ongoingTasksItem:
            "Ongoing tasks defined on a database can use the server-wide connection strings that are available to that database.",
        deleteItem: "Connection strings that are used by ongoing tasks cannot be deleted.",
        exportItem:
            "Server-wide connection strings are not included when exporting a database or restoring it from backup.",
        overviewDocsLink: "Docs - Connection Strings Overview",
        serverWideDocsLink: "Docs - Connection Strings Server-wide",
    },
    featureAvailability: {
        serverWide: "Server-Wide Connection Strings",
    },
} as const;
