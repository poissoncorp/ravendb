export default {
    title: "Data Archival",
    loadError: "Unable to load data archival",
    saveSuccess: "Data archival configuration saved successfully",
    enableSwitch: "Enable Data Archival",
    customFrequencySwitch: "Set custom archive frequency",
    frequencyPlaceholder: "Default ({{seconds}})",
    secondsAddon: "seconds",
    maxItemsSwitch: "Set max number of documents to process in a single run",
    itemsAddon: "items",
    about: {
        intro: "When <strong>Data Archival</strong> is enabled:",
        scanItem:
            "The server scans the database at the specified <strong>frequency</strong>, searching for documents that should be archived.",
        metadataItem:
            "Any document that has an <code>@archive-at</code> metadata property whose time has passed at the time of the scan will be archived:",
        compressedItem: "The archived document will be compressed",
        replacedItem:
            "The <code>@archive-at</code> metadata property will be replaced by: <code>@archived: true</code>",
        indexingItem:
            "Per-index/subscription, you can configure whether archived documents will be included in the indexing and subscription processes",
        exampleLabel: "Example of a document scheduled for archival:",
        docsLink: "Docs - Data Archival - Overview",
    },
} as const;
