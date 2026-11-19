export default {
    title: "Tombstones",
    loadFailed: "Unable to load tombstones",
    forceCleanup: "Force cleanup",
    forceCleanupConfirmTitle: "Do you want to force tombstones cleanup?",
    alert: {
        schedule:
            "By default, tombstones cleanup is scheduled to be carried out by the server every 5 minutes, unless configured otherwise.",
        forceCleanup:
            "Upon clicking <strong>Force Cleanup</strong>, the action will be executed immediately - any tombstone that can be removed will be deleted.",
    },
    minimumEtags: {
        documents: "Minimum all document Etags",
        timeSeries: "Minimum all timeseries Etags",
        counters: "Minimum all counter Etags",
    },
    loadingPlaceholder: "Loading placeholder",
    perCollection: {
        title: "Per Collection - Max Etags that can be deleted",
        collection: "Collection",
        documentTask: "Document Task",
        documentEtag: "Document Etag",
        timeSeriesTask: "Time Series Task",
        timeSeriesEtag: "Time Series Etag",
        counterTask: "Counter Task",
        counterEtag: "Counter Etag",
    },
    perTask: {
        title: "Per Task - Max Etag that can be deleted",
        process: "Process",
        name: "Name",
        numberOfTombstonesLeft: "Number of tombstones left",
        tombstoneTypes: "Tombstone types",
        tombstoneTypesDocuments: "Documents: {{documents}}",
        tombstoneTypesAll: "Documents: {{documents}}, TimeSeries: {{timeSeries}}, Counters: {{counters}}",
        collection: "Collection",
        etag: "Etag",
        cleanupStatus: "Cleanup status",
        blocking: "Blocking",
        notBlocking: "Not blocking",
    },
    etag: {
        maxValue: "(max value)",
        noneRemovableTitle: "No tombstones can be removed",
        removableUpToTitle: "Can remove tombstones for Etags <= {{etag}}",
        anyRemovableTitle: "Can remove any tombstone",
    },
} as const;
