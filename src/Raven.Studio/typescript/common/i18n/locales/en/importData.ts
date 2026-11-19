export default {
    title: "Import data",
    description: "Choose an import option that best suits your needs.",
    options: {
        file: {
            title: "From file (.ravendbdump)",
            description:
                "Restore a full database, including documents, indexes, and settings, from a single .ravendbdump backup file.",
        },
        ravenDb: {
            title: "From RavenDB Server",
            description:
                "Migrate chosen collections, documents, and indexes directly from another active RavenDB server instance.",
        },
        csv: {
            title: "From CSV File",
            description:
                "Import documents into a new or existing collection by mapping columns from a CSV file to document properties.",
        },
        sql: {
            title: "From SQL",
            description:
                "Migrate data from a SQL database using a connection string and a custom script to transform tables into documents.",
        },
        noSql: {
            title: "From NoSQL",
            description: "Import documents from Azure Cosmos DB.",
        },
    },
} as const;
