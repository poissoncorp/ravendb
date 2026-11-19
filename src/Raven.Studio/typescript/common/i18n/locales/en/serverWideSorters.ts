export default {
    title: "Server-Wide Sorters",
    testInfo: "To test server-wide sorters, go to the Custom Sorters view in a database",
    addButton: "Add a server-wide custom sorter",
    heading: "Server-wide custom sorters",
    loadError: "Unable to load custom sorters",
    empty: "No server-wide custom sorters have been defined",
    item: {
        newNameFallback: "New sorter name",
        saveButton: "Save changes",
        discardButton: "Discard",
        nameLabel: "Name",
        namePlaceholder: "Enter a sorter name",
        loadFromFile: "Load from a file",
    },
    about: {
        sorters:
            "A <strong>Custom Sorter</strong> allows you to define how documents will be ordered in the query results<br/>according to your specific requirements.",
        inThisView: "<strong>In this view</strong>, you can add your own sorters:",
        allDatabasesItem: "The custom sorters added here can be used with queries in ALL databases in your cluster.",
        coraxItem: "Note: custom sorters are not supported when querying Corax indexes.",
        provideCode: "Provide <code>C#</code> code in the editor view, or upload from file:",
        nameItem: "The sorter name must be the same as the sorter's class name in your code.",
        inheritItem: "Inherit from <code>Lucene.Net.Search.FieldComparator</code>",
        compilableItem: "Code must be compilable and include all necessary <code>using</code> statements.",
        docsLink: "Docs - Custom Sorters",
    },
    featureAvailability: {
        serverWide: "Server-Wide Custom Sorters",
    },
} as const;
