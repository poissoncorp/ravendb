export default {
    confirm: {
        no: "No",
        yes: "Yes",
    },
    documentFetchFailed: "Failed to fetch document: {{id}}",
    progress: {
        overall: "Overall progress",
        timeLeft: "Estimated time left: {{duration}}",
        timeLeft_withRate: "Estimated time left: {{duration}} ({{rate}} / sec)",
        processedAll: "Processed all items ({{total}})",
        processed: "{{processed}} out of {{total}} ({{left}} left)",
    },
    sqlSyntax: {
        example: "Example: <code>{{connectionString}}</code>",
        moreExamples: "More examples can be found in <a>full syntax reference</a>",
    },
} as const;
