export default {
    notAuthorized: "You are not authorized to view this page!",
    filterPlaceholder: "Filter Keys",
    refreshTitle: "Reload settings",
    edit: "Edit",
    pendingValuesNote: "Note: Pending values will only be in effect once the database is reloaded",
    reload: {
        title: "There are a few ways to reload the database:",
        restartDatabase: "Restart the database from the databases view in the Studio.",
        disableAndEnable:
            "Disable and then enable the database from the databases view in the Studio.<br/>This will reload the database on ALL cluster nodes immediately.",
        restartNodes:
            "Restart RavenDB on all nodes.<br/>The database settings configuration will become effective per node that is restarted.",
    },
    columns: {
        category: "Category",
        configurationKey: "Configuration Key",
        value: "Value",
        effectiveValueInUse: "Effective Value in Use",
        pendingValue: "Pending Value",
        effectiveValue: "Effective Value",
        origin: "Origin",
    },
    origin: {
        database: "Database",
        server: "Server",
        default: "Default",
    },
    originTitle: {
        database: "Value has been customized for the database, overriding the server & default settings",
        server: "Value has been customized for the server, overriding the default settings",
        default: "No customized value has been set",
    },
    customizedEntriesCountTitle: "Number of customized entries in category",
    noMatchingCategory: "No matching category",
    noMatchingEntries: "No matching entries.",
    entry: {
        unauthorized: "Unauthorized to access value!",
        sourceTitle_server: "This is a server-wide configuration",
        sourceTitle_default: "This is the default configuration",
        serverWideOnlyTitle:
            "This is a server-wide entry.<br/>It can be edited from the <strong>settings.json</strong> file located in your RavenDB executable folder.",
        overrideTitle: "Customize the database value",
        override: "Override",
        pendingTitle: "This configuration will be in effect only after reloading the database",
        pending: "This configuration is pending",
        valuePlaceholder: "Enter a new value (or leave empty for null)",
        numberPlaceholder_nullable: "Enter a new number (or leave empty for null)",
        numberPlaceholder_notNullable: "Enter a new number",
        pathPlaceholder: "Enter a new path",
        timePlaceholder_nullable: "Enter a new time (or leave empty for null)",
        timePlaceholder_notNullable: "Enter a new time",
        sizePlaceholder_nullable: "Enter a new size (or leave empty for null)",
        sizePlaceholder_notNullable: "Enter a new size",
        setDefaultTitle: "Use the default value",
        setDefault: "Set default",
    },
    refreshed: "Database Settings was successfully refreshed",
    unsavedChanges: {
        title: "Unsaved changes",
        message: "You have unsaved changes. How do you want to proceed?",
        stayButton: "Stay in edit mode",
        discardButton: "Discard changes",
    },
    editModeConfirm: {
        title: "Are you an expert?",
        message: "Modify the database settings only if you know what you are doing.",
        okButton: "OK",
    },
    saveConfirm: {
        note: "Note:",
        reloadRequired:
            "After saving, the database must be reloaded in order for these configuration changes to be in effect.",
        notAppliedWithoutReload:
            "Without reloading the database, the changes will NOT apply (despite being written to the database record).",
        settingsToSave: "The following database settings will be saved:",
    },
} as const;
