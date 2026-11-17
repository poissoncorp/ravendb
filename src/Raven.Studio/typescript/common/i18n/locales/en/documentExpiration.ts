export default {
    title: "Document Expiration",
    loadError: "Unable to load document expiration",
    saveSuccess: "Expiration configuration saved successfully",
    enableSwitch: "Enable Document Expiration",
    customFrequencySwitch: "Set custom expiration frequency",
    frequencyPlaceholder: "Default ({{value}})",
    limitWarning_one: "Your current license does not allow an expiration frequency below {{count}} hour.",
    limitWarning_other: "Your current license does not allow an expiration frequency below {{count}} hours.",
    maxItemsSwitch: "Set max number of documents to process in a single run",
    itemsAddon: "items",
    minCheckFrequency: "Min check frequency (hrs)",
    about: {
        intro: "When <strong>Document Expiration</strong> is enabled:",
        scanItem:
            "The server scans the database at the specified <strong>frequency</strong>, searching for documents that should be deleted.",
        metadataItem:
            "Any document that has an <code>@expires</code> metadata property whose time has passed at the time of the scan will be removed.",
        exampleLabel: "Example of a document scheduled for expiration:",
        docsLink: "Docs - Document Expiration",
    },
} as const;
