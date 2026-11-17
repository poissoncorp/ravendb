export default {
    title: "Document Compression",
    loadError: "Unable to load document compression configuration",
    storageReportLink: "Storage Report",
    compressedDocuments: "Documents that will be compressed:",
    newDocumentsItem_all: "New documents created in all collections",
    newDocumentsItem_selected: "New documents created in the selected collections",
    modifiedDocumentsItem_all: "Existing documents that are modified & saved in all collections",
    modifiedDocumentsItem_selected: "Existing documents that are modified & saved in the selected collections",
    compressRevisionsSwitch: "Compress revisions for all collections",
    about: {
        storageItem:
            "Enable documents compression to achieve efficient data storage.<br/>Storage space will be reduced using the zstd compression algorithm.",
        scopeItem: "Documents compression can be set for all collections, selected collections, and all revisions.",
        docsLink: "Docs - Document Compression",
    },
} as const;
