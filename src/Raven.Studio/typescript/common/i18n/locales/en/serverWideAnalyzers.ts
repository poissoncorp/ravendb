export default {
    title: "Server-Wide Analyzers",
    addButton: "Add a server-wide custom analyzer",
    heading: "Server-wide custom analyzers",
    loadError: "Unable to load custom analyzers",
    empty: "No server-wide custom analyzers have been defined",
    item: {
        newNameFallback: "New analyzer name",
        saveButton: "Save changes",
        discardButton: "Discard",
        nameLabel: "Name",
        namePlaceholder: "Enter analyzer name",
        loadFromFile: "Load from a file",
    },
    about: {
        analyzers:
            "<strong>Analyzers</strong> are used by indexes to split the index-fields into tokens (terms).<br/>The analyzer defines how the field is tokenized.<br/>When querying an index, these terms are used to define the search criteria and filter query results.",
        inThisView:
            "<strong>In this view</strong>, you can add your own analyzers in addition to the existing analyzers that come with RavenDB. The custom analyzers added here can be used by indexes in ALL databases in your cluster.",
        provideCode: "Provide <code>C#</code> code in the editor view, or upload from file.",
        nameItem: "The analyzer name must be the same as the analyzer's class name in your code.",
        inheritItem: "Inherit from <code>Lucene.Net.Analysis.Analyzer</code>",
        compilableItem: "Code must be compilable and include all necessary <code>using</code> statements.",
        docsLink: "Docs - Custom Analyzers",
    },
    featureAvailability: {
        serverWide: "Server-Wide Custom Analyzers",
    },
} as const;
