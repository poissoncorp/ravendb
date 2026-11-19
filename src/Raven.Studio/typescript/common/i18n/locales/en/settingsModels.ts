export default {
    connectionString: {
        nameAlreadyUsed: "Connection string name is already used",
    },
    authenticationType: {
        basic: "Basic",
        connectionString: "Connection String",
        passwordless: "Passwordless",
    },
    azureQueueStorage: {
        requiredFields: "Please define all required fields: DefaultEndpointsProtocol, AccountName and AccountKey",
    },
    elasticSearch: {
        auth: {
            none: "No authentication",
            basic: "Basic",
            apiKey: "API Key",
            encodedApiKey: "Encoded API Key",
            certificate: "Certificate",
        },
        certificateNotUploaded: "Certificate not uploaded",
        uploadCertificateFailed: "Unable to upload certificate",
        nodeUrlRequired: "At least one Elasticsearch node URL is required. Enter URL and click Add.",
    },
    kafka: {
        keyAndValueRequired: "Both key and value are required",
        bootstrapServerNoProtocol: "A bootstrap server cannot start with http/https",
        bootstrapServersFormat: "Format should be: 'hostA:portNumber,hostB:portNumber,...'",
        serverCertificateInfo:
            "The following <strong>configuration options</strong> will be set for you when using RavenDB server certificate:",
    },
    olap: {
        destinationRequired: "Please select at least one destination",
    },
    raven: {
        discoveryUrlRequired: "At least one discovery url is required",
    },
    sql: {
        deprecatedProvider: "DEPRECATED: {{provider}}",
        connectionStringPlaceholder: "Enter connection string",
        connectionStringForProviderPlaceholder: "Enter the complete connection string for the {{provider}}",
    },
    databaseSettings: {
        noDescription: "No description is available",
        databaseValueDeleted: "<Database value deleted>",
        minValue: "Please enter a value greater or equal to {{min}}",
    },
    discoveryUrl: {
        httpsNotSupported: "Connecting from unsecured server (http) to secured (https) is not supported.",
    },
} as const;
