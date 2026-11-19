export default {
    title: "Index Cleanup",
    loadError: "Unable to load index cleanup data",
    about: {
        performance:
            "Maintaining multiple indexes can lower performance. Every time data is inserted, updated, or deleted, the corresponding indexes need to be updated as well, which can lead to increased write latency.",
        recommendations:
            "To counter these performance issues, RavenDB recommends a set of actions to optimize the number of indexes. Note that when merging or removing indexes, you need to update the index reference in your application.",
        docsLink: "Docs - Index Cleanup",
    },
    featureAvailability: {
        indexCleanup: "Index Cleanup",
    },
    nav: {
        mergeIndexesAlt: "Merge indexes",
        mergeIndexes: "Merge<br/>indexes",
        removeSubindexesAlt: "Remove subindexes",
        removeSubindexes: "Remove<br/>sub-indexes",
        removeUnusedAlt: "Remove unused indexes ",
        removeUnused: "Remove <br/>unused indexes",
        unmergeableAlt: "Unmergeable indexes",
        unmergeable: "Unmergeable<br/>indexes",
        mergeSuggestionsErrorsAlt: "Merge suggestion errors",
        mergeSuggestionsErrors: "Merge suggestions<br/>errors",
    },
    columns: {
        indexName: "Index name",
        lastQueryTime: "Last query time",
        lastIndexingTime: "Last indexing time",
    },
    mergeIndexes: {
        title: "Merge indexes",
        description:
            "Combining several indexes with similar purposes into a single index can reduce the number of times that data needs to be scanned.<br/>Once a <strong>NEW</strong> merged index definition is created, the original indexes can be removed.",
        empty: "No indexes to merge",
        reviewButton: "Review suggested merge",
    },
    removeSubindexes: {
        title: "Remove sub-indexes",
        description:
            "If an index is completely covered by another index (i.e., all its fields are present in the larger index) maintaining it does not provide any value and only adds unnecessary overhead. You can remove the subset index without losing any query optimization benefits.",
        empty: "No subset indexes",
        deleteButton: "Delete selected sub-indexes",
        subindex: "Sub-index",
        containingIndex: "Containing index",
        lastQueryTime: "Last query time (sub-index)",
        lastIndexingTime: "Last indexing time (sub-index)",
    },
    removeUnused: {
        title: "Remove unused indexes",
        description:
            "Unused indexes still consume resources. Indexes that have not been queried for over a week are listed below. Review the list and consider deleting any unnecessary indexes.",
        empty: "No unused indexes",
        deleteButton: "Delete selected indexes",
        unusedIndex: "Unused index",
    },
    unmergeable: {
        title: "Unmergeable indexes",
        description:
            "The following indexes cannot be merged. See the specific reason explanation provided for each index.",
        empty: "No unmergeable indexes",
        reason: "Unmergeable reason",
    },
    mergeSuggestionsErrors: {
        title: "Merge suggestions errors",
        description: "The following errors have been encountered when trying to create index merge suggestions.",
        error: "Error",
        hideDetails: "Hide details",
        showDetails: "Show details",
        copied: "Error has been copied to clipboard",
    },
    deleteConfirm: {
        title_one: "Delete index?",
        title_other: "Delete indexes?",
        success_one: "Successfully deleted {{count}} indexes!",
        success_other: "Successfully deleted {{count}} indexes!",
    },
} as const;
