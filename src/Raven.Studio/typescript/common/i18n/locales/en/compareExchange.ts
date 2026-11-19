export default {
    addNewItem: "Add new item",
    deleteButton: "Delete",
    deleteButton_all: "Delete (all)",
    deleteButton_selected: "Delete ({{selectedCount}})",
    deleteAllConfirm: {
        title: "Delete ALL compare exchange items?",
        message: "You're about to delete <strong>ALL</strong> compare exchange items.",
        message_filtered:
            "You're about to delete <strong>ALL</strong> compare exchange items matching the current filter.",
        confirmButton: "Delete All",
    },
    deleteConfirm: {
        title_one: "Delete {{formattedCount}} compare exchange item?",
        title_other: "Delete {{formattedCount}} compare exchange items?",
    },
    columns: {
        key: "Compare Exchange Key",
        keyFilterPlaceholder: "Key starts with...",
        value: "Value",
        metadata: "Metadata",
        raftIndex: "Raft Index",
    },
    about: {
        heading: "About this view",
        description: "Get additional info on this feature",
        intro: "<strong>Compare exchange</strong> items are cluster-wide key/value pairs that provide atomic, interlocked compare-exchange operations across the database group.",
        consistency: "Their values are kept consistent database-wide via the Raft consensus algorithm.",
        inThisView: "This view displays all compare exchange items.",
        keyColumn:
            '<strong>"Compare Exchange Key"</strong> column:<br/>The unique key of the item. Click the key to edit the item.',
        valueColumn: '<strong>"Value"</strong> column:<br/>The value stored for this key.',
        metadataColumn: '<strong>"Metadata"</strong> column:<br/>Optional metadata associated with the item.',
        raftIndexColumn:
            '<strong>"Raft Index"</strong> column:<br/>The version of the item, as set by the cluster consensus. This is used for concurrency control.',
        actions:
            "You can add a new item, filter items by key prefix, and sort the loaded rows.<br/>Select and delete items – selecting all will delete all items matching the current filter.",
        docsLink: "Docs - Compare Exchange",
    },
} as const;
