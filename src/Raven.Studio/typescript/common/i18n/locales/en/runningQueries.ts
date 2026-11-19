export default {
    filterPlaceholder: "Filter by index name or query",
    databasesSelector: {
        nSelected: " databased selected",
        allSelected: "All databases selected",
    },
    monitoring: "Monitoring (tail -f)",
    columns: {
        abort: "Abort",
        abortTitle: "Abort this query",
        databaseName: "Database Name",
        indexName: "Index Name",
        duration: "Duration",
        streaming: "Streaming",
        query: "Query",
        details: "Details",
    },
    durationMs: "{{duration}} ms",
    empty: "No running queries for now.",
    abortConfirm: {
        title: "Abort the query",
        message: "Do you want to abort query for index: {{indexName}}?",
    },
    aborting: "Aborting query",
} as const;
