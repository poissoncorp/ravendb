export default {
    about: {
        intro: "Reduce the database storage space by enabling document compression from this view.",
        canBeSetFor: "Document compression can be set for:",
        documentsItem: "Documents in all collections or selected collections.",
        revisionsItem: "Revisions in all collections.",
        notApplied:
            "Compression is not applied to attachments, counters, and time series data, only to the content of documents and revisions.",
        triggeredWhen:
            "When enabled, compression will be triggered by the server when either of the following occurs for the configured collections:",
        storingItem: "Storing new documents.",
        modifyingItem: "Modifying & saving existing documents.",
        compactItem: "When a compact operation is initiated by the Client API, existing documents will be compressed.",
        docsLink: "Docs - Document Compression",
    },
} as const;
