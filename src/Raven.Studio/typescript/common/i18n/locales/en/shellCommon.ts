export default {
    accessLevel: {
        clusterAdminOrNode: "Cluster Admin/Node",
        operator: "Operator",
        admin: "Admin",
        readWrite: "Read/Write",
        readOnly: "Read Only",
    },
    disableReason: {
        insufficientSecurityClearance: "Insufficient security clearance",
        insufficientDatabaseAccess: "Insufficient database access",
        required: "Required: <strong>{{accessLevel}}</strong>",
    },
    activeDatabase: {
        disabled: "Database {{databaseName}} was disabled",
        notRelevant: "Database {{databaseName}} is not longer relevant on this node",
    },
    activation: {
        disabledTitle: "Database '{{databaseName}}' is disabled!",
        disabledDetails: "You can't access any section of the database while it's disabled.",
        notRelevantTitle: "Database '{{databaseName}}' is not relevant on this node!",
        notRelevantDetails: "You can't access any section of the database while it's not relevant.",
        notFound: "The database {{databaseName}} doesn't exist!",
    },
    logout: {
        title: "Log out",
        confirmation: "Are you sure you want to log out?",
    },
} as const;
