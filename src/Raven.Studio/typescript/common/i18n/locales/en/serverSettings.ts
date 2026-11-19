export default {
    title: "Server Settings",
    noAccess: {
        badge: "Insufficient access",
        message: "You are not authorized to view this page",
    },
    loadError: "Error during loading server settings",
    columns: {
        configurationKey: "Configuration Key",
        effectiveValue: "Effective Value",
        origin: "Origin",
    },
    origin: {
        serverDescription: "Value has been customized, overriding the default settings",
        defaultDescription: "No customized value has been set",
    },
    about: {
        intro: "The <strong>Server Settings</strong> view displays all configuration keys for the server.",
        configurationKeyItem:
            '<strong>Configuration Key</strong>:<br/> The key appears "Orange" if a value has been customized.<br/> The key appears "Green" if the default value is in use.',
        effectiveValueItem:
            "<strong>Effective Value</strong>:<br/> The active value currently applied for the configuration key.",
        originItem:
            '<strong>Origin</strong>:<br/> "Server" - indicates the configuration key was customized.<br/> "Default" - indicates no customized value has been set.',
        readOnly:
            "This view is read-only.<br/> Server-wide scope values can be customized via environment variables, the <i>settings.json</i> file, or command-line arguments, as explained in the <a>configuration overview</a>.",
        databaseOverride:
            "Some configuration keys can be overridden by a database-specific value,<br/> which will take precedence over the server-wide configuration key for that database.<br/> To modify configuration keys for a specific database go to the <strong>Database Settings</strong> View.",
        docsLink: "Docs - Configuration Overview",
    },
} as const;
