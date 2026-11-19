export default {
    deleteConfirm: {
        intro_one: "You are deleting index:",
        intro_other: "You are deleting <strong>{{count}}</strong> indexes:",
        reduceResultsCollections: "(Reduce Results Collections: {{reduceOutputCollection}})",
        reduceResultsCollections_withReference:
            "(Reduce Results Collections: {{reduceOutputCollection}}, {{referenceCollection}})",
        skippingLocked: "Skipping locked indexes:",
        reduceResultsWarning: "'Reduce Results Collections' were created by the index(es).",
        reduceResultsNote:
            "<strong>Note</strong>:<br/>Clicking 'Delete' will remove the index(es), but not the Reduce Results Collection(s). You need to manually remove the documents from these collections.",
        reduceResultsImportant:
            "<strong>Important</strong>:<br/>If running in a cluster, these documents must be deleted from <strong>each node individually</strong>, because their deletion is not replicated between nodes.",
    },
} as const;
