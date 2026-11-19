export default {
    notAvailableSharded: "Integrations are not available for <icon/> sharded databases",
    title: "Integrations",
    postgreSqlHeading: "PostgreSQL Protocol Credentials",
    addNew: {
        licenseRestricted: "Your license doesn't allow adding new credentials.",
        noPermissions: "You don't have permissions to add new credentials.",
        title: "Add new credentials",
        button: "Add new",
    },
    list: {
        loadError: "Unable to load credentials",
        empty: "No credentials have been defined",
    },
    item: {
        deleteConfirmTitle: "Delete credentials?",
        deleteConfirmMessage: "You're deleting PostgreSQL credentials for user: {{username}}",
        newName: "New credentials",
        save: "Save credentials",
        discardTitle: "Discard changes",
        discard: "Discard",
        deleteTitle: "Delete credentials",
        usernameLabel: "Username",
        usernamePlaceholder: "Enter your username",
        passwordLabel: "Password",
        passwordPlaceholder: "Enter your password",
        generatePasswordTitle: "Generate a random password",
        generatePassword: "Generate password",
        copyTitle: "Copy to clipboard",
        passwordCopied: "Password was copied to clipboard.",
    },
    alerts: {
        licenseUpgradeRequired:
            "To use this feature, your license must include either of the following features: PostgreSQL integration or Power BI.",
        licensingOptions: "Licensing options",
        postgreSqlNotEnabled:
            "PostgreSQL support must be explicitly enabled in your <code>settings.json</code> file. Learn more <a>here</a>.",
    },
    about: {
        credentials:
            "In this view, you can define credentials that third-party clients are required to provide when connecting to RavenDB.",
        secureServer: "Providing the credentials is only required of clients when RavenDB is installed as a Secure Server.",
        postgreSqlSupport: "PostgreSQL support:",
        postgreSqlProtocol:
            "RavenDB implements the PostgreSQL protocol, allowing clients that use PostgreSQL (e.g. Power BI) to retrieve data from a RavenDB database.",
        verifyLicense:
            "Verify your license supports the PostgreSQL Protocol.<br/>The PostgreSQL protocol support must also be explicitly enabled in your RavendB's settings.",
        docsLink: "Docs - Integrations",
    },
    featureAvailability: {
        postgreSql: "PostgreSQL",
        powerBi: "Power BI",
    },
} as const;
