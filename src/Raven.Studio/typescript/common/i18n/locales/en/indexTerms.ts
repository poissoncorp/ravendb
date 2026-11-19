export default {
    loadError: "Error during getting fields",
    title: "Index terms for <a>{{indexName}}</a>",
    loaded_one: "{{count}} loaded",
    loaded_other: "{{count}} loaded",
    noFields: "No fields were found",
    field: {
        dynamic: "Dynamic field",
        vector: "Vector field",
        noEntries: "No entries were found.",
        loadMore: "Load more",
    },
    term: {
        queryName: "Index terms for {{indexName}} ({{fieldName}}: {{term}})",
        queryTitle: "Query index with given term",
        previewTitle: "Preview item",
        copied: "Index term was copied to clipboard",
        copyTitle: "Copy to clipboard",
    },
    preview: {
        title: "Indexing term value",
        previous: "Previous",
        next: "Next",
        close: "Close",
    },
} as const;
