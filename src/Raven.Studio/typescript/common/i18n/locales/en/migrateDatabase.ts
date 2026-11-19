export default {
    sharded: {
        featureNotAvailable: "Feature not available",
        notAvailable: "Migrate data from another database is not available for <icon/> sharded databases",
    },
    title: "Migrate data from another database",
    overwriteWarning: "Note: Migration will overwrite any existing documents.",
    migratorPath: {
        label: "Migrator Path",
        info: "For RavenDB version 4.x and up, the <strong>Raven.Migrator</strong> executable can be found in the <strong>Tools</strong> package.<br/>The Tools package can be downloaded from the <a>ravendb.net</a> website.",
        definedByAdministrator: "Path for Raven.Migrator executable was defined by administrator",
        deployTools: "Please deploy RavenDB Tools via <a>RavenDB Cloud Portal</a>",
        placeholder: "Enter the full path of the directory containing the Raven.Migrator executable",
        foundTitle: "Raven.Migrator was found under specified path",
    },
    databaseSource: "Database Source",
    azureEndpointUrl: "Azure Endpoint URL",
    primaryKey: "Primary Key",
    databaseName: "Database Name",
    migrateAllCollections: "Migrate all collections",
    collections: "Collections",
    transformScript: {
        toggle: "Use Transform script",
        info: "Transform scripts are written in JavaScript",
    },
    submit: {
        all: "Migrate all collections",
        selected_one: "Migrate {{formattedCount}} collection",
        selected_other: "Migrate {{formattedCount}} collections",
    },
} as const;
