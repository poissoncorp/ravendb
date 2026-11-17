export default {
    title: "Studio Configuration",
    loadError: "Unable to load studio configuration",
    serverWideLink: "Go to Server-Wide Studio Configuration View",
    serverWideLinkTitle: "Navigate to the server-wide Client Configuration View",
    environment: {
        label: "Database Environment",
        tagInfo: "Apply a <strong>tag</strong> to the Studio indicating the database environment.",
        noEffectInfo: "This does not affect any settings or features.",
    },
    autoIndexes: {
        disableSwitch: "Disable creating new Auto-Indexes",
        dynamicQueryInfo:
            "Toggle on to disable creating new Auto-Indexes when making a <strong>dynamic query</strong>.",
        existingIndexInfo: "Query results will be returned only when a matching Auto-Index already exists.",
    },
    about: {
        scope: "This is the <strong>Database Studio-Configuration</strong> view.<br/>The available configuration options will apply only to this database.",
        docsLink: "Docs - Studio Configuration",
    },
} as const;
