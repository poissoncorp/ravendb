export default {
    taskTypes: {
        externalReplication: "External Replication",
        replicationHub: "Replication Hub",
        replicationSink: "Replication Sink",
        azureQueueStorageEtl: "Azure Queue Storage ETL",
        amazonSqsEtl: "Amazon SQS ETL",
    },
    connectionStatus: {
        active: "Active",
        reconnect: "Reconnect",
        notActive: "Not Active",
        notOnNode: "Not On Node",
        none: "None",
    },
    state: {
        enabled: "Enabled",
        disabled: "Disabled",
        partial: "Partial",
    },
    delayDescription: "Documents will be replicated after a delay time of <strong>{{delayTimeHumane}}</strong>",
    applyToAllCollections: "Apply to All Collections",
    validation: {
        includeOptionRequired: "Note: At least one 'include' option must be checked...",
        transformationScriptNotDefined: "Transformation Script is Not defined",
        collectionRequired: "At least one collection is required",
        valueGreaterThanZero: "Please enter a value greater than 0",
        replicationModeRequired: "Please select at least one replication mode",
    },
    exportDatabase: {
        itemsToWarnAbout: {
            counters: "Counters",
            timeSeries: "Time Series",
            revisions: "Revisions",
        },
    },
    migrateRavenDb: {
        versionNotAvailable: "N/A",
        versionUnknown: "Unknown",
        serverVersion: "{{majorVersion}} (build: {{buildVersion}})",
        resourceType_fileSystem: "File system",
        resourceType_database: "Database",
        unauthorized: "Unauthorized access to the server, please enter your credentials",
        fieldRequired: "This field is required.",
        configurationOptionRequired: "Note: At least one 'configuration' option must be checked...",
    },
    elasticSearchEtl: {
        indexNotDefined: "Elasticsearch index is Not defined",
    },
    snowflakeEtl: {
        tableNotDefined: "Snowflake table is Not defined",
    },
    sqlEtl: {
        tableNotDefined: "SQL table is Not defined",
    },
    queueEtl: {
        duplicateQueueNames: "Duplicate queue names",
    },
    queueSink: {
        scriptNotDefined: "Script is Not defined",
        queueRequired: "At least one queue is required",
    },
    embeddingsGeneration: {
        chunkingMethods: {
            plainTextSplit: "Plain Text: Split",
            plainTextSplitLines: "Plain Text: Split Lines",
            plainTextSplitParagraphs: "Plain Text: Split Paragraphs",
            markdownSplitLines: "Markdown: Split Lines",
            markdownSplitParagraphs: "Markdown: Split Paragraphs",
            htmlStrip: "HTML: Strip",
        },
        quantizationTypes: {
            single: "Single (no quantization)",
            int8: "Int8",
            binary: "Binary",
        },
        selectQuantizationType: "Select quantization type",
        selectChunkingMethod: "Select chunking method",
        embeddingsSource: {
            script: "Script",
            paths: "Paths",
        },
        identifierFormat: "Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed.",
        pathRequired: "At least one path is required",
        overlapTokensTooLarge: "Overlap tokens must be less than or equal to max tokens per chunk",
        pathExists: "Path already exists",
    },
    subscription: {
        changeVectorRequired: "Please enter change vector",
    },
    replicationAccess: {
        filteringPathRequired: "Please add at least one filtering path",
        filteringPathOrHubToSinkRequired: "Please add at least one filtering path, or use the Hub to Sink paths",
        singleDocumentPatternWarning:
            "Path patterns that do not end with * (asterisk) will match only a single document",
        hubToSinkPaths_one: "Hub to Sink ({{count}} path)",
        hubToSinkPaths_other: "Hub to Sink ({{count}} paths)",
        sinkToHubPaths_one: "Sink to Hub ({{count}} path)",
        sinkToHubPaths_other: "Sink to Hub ({{count}} paths)",
        bothDirectionsPaths: "{{hubToSink}}, {{sinkToHub}}",
        exportBeforeSaving: "Export the Access Configuration or download the certificate before saving.",
        certificateSource_server: "Use the server certificate",
        certificateSource_own: "Provide your own certificate",
        certificateNotExtracted: "Could not extract certificate from this file",
        privateKeyRequired: "A certificate with a private key is required",
        extractCertificateFailed:
            "Unable to extract certificate from file. Verify file is .pfx containing a single private key and check provided password.",
        prefixPathWildcard: "When using '*' as the last character, the previous character must be '/' or '-'",
        certificateExpired: "Expired {{date}}",
    },
    serverWide: {
        noDatabasesAdded: "No databases added",
    },
    smugglerDatabaseRecord: {
        connectionStringsNotSelected: {
            ravenDb: "RavenDB Connection strings were not selected.",
            sql: "SQL Connection strings were not selected.",
            snowflake: "Snowflake Connection strings were not selected.",
            olap: "OLAP Connection strings were not selected.",
            elasticsearch: "Elasticsearch Connection strings were not selected.",
            queue: "Queue Connection strings were not selected.",
            ai: "AI Connection strings were not selected.",
        },
    },
    sqlMigration: {
        attachment: "Attachment",
        mySqlDataProvider: "DEPRECATED: MySQL Server (MySql.Data.MySqlClient)",
    },
    backup: {
        title_manual: "Create a Backup",
        title_new: "New Periodic Backup",
        title_edit: "Edit Periodic Backup",
        uploadModes: {
            default: "Default",
            directUpload: "Direct Upload",
        },
        noDestination: "No destination is defined",
        fullOrIncrementalRequired: "Please select either full or incremental backup",
        compressionLevels: {
            optimal: "Optimal",
            fastest: "Fastest",
            noCompression: "No Compression",
            smallestSize: "Smallest Size",
        },
        compressionAlgorithms: {
            default: "Default",
        },
        retention: {
            noBackupsRemoved: "No backups will be removed.",
            backupsRemoved:
                "Backups that are older than <strong>{{retentionTimeHumane}}</strong> will be removed on the next scheduled backup task",
            minimumPeriod: "Retention period must be greater than 1 day",
        },
        encryption: {
            modes: {
                useDatabaseKey: "Encrypt using Database Encryption Key",
                useProvidedKey: "Provide your own Encryption Key",
            },
            keySourceTitle: {
                select: "Select Encryption Key mode",
                snapshotUsesDatabaseKey: "The Database Encryption Key will be used when selecting Snapshot type",
                databaseNotEncrypted: "The database is not encrypted. Provide your own Encryption Key for the Backup task.",
            },
            confirmUnencryptedBackup: "Please confirm you want to perform unencrypted backup of encrypted database",
            backupTypeNotSelected: "Backup Type was not selected. Select Backup Type to continue.",
            snapshotUnencryptedNotAllowed:
                "A 'Snapshot' backup-type was selected. An Unencrypted backup can only be defined for Encrypted databases when a 'Backup' backup-type is selected.",
            snapshotEncryptedNotSupported:
                "A 'Snapshot' backup-type was selected. Creating an Encrypted backup for Unencrypted databases is only supported when selecting the 'Backup' backup-type.",
        },
        awsRegions: {
            afSouth1: "Africa (Cape Town)",
            apEast1: "Asia Pacific (Hong Kong)",
            apSouth2: "Asia Pacific (Hyderabad)",
            apSoutheast3: "Asia Pacific (Jakarta)",
            apSoutheast4: "Asia Pacific (Melbourne)",
            apSouth1: "Asia Pacific (Mumbai)",
            apNortheast3: "Asia Pacific (Osaka)",
            apNortheast2: "Asia Pacific (Seoul)",
            apSoutheast1: "Asia Pacific (Singapore)",
            apSoutheast2: "Asia Pacific (Sydney)",
            apNortheast1: "Asia Pacific (Tokyo)",
            caCentral1: "Canada (Central)",
            cnNorth1: "China (Beijing)",
            cnNorthwest1: "China (Ningxia)",
            euCentral1: "Europe (Frankfurt)",
            euWest1: "Europe (Ireland)",
            euWest2: "Europe (London)",
            euSouth1: "Europe (Milan)",
            euWest3: "Europe (Paris)",
            euNorth1: "Europe (Stockholm)",
            euSouth2: "Europe (Spain)",
            euCentral2: "Europe (Zurich)",
            ilCentral1: "Israel (Tel Aviv)",
            meSouth1: "Middle East (Bahrain)",
            meCentral1: "Middle East (UAE)",
            saEast1: "South America (São Paulo)",
            usEast1: "US East (N. Virginia)",
            usEast2: "US East (Ohio)",
            usWest1: "US West (N. California)",
            usWest2: "US West (Oregon)",
            usGovEast1: "AWS GovCloud (US-East)",
            usGovWest1: "AWS GovCloud (US-West)",
        },
        invalidRegion: "Invalid region",
        regionFormat: "AWS Region must include a '-' and cannot start or end with it",
        info: {
            differences: "Differences between Backup and Snapshot:",
            data: "Data",
            dataBackup:
                "<strong>Backup</strong> includes documents, identities, and index definitions.<br />It doesn't include index data - indexes are rebuilt from the backed-up definitions when restoring the database.",
            dataSnapshot:
                "<strong>Snapshot</strong> contains the raw data including the indexes (definitions and data, or definitions only).",
            indexDataDifferences: "Differences when Snapshot backs up both index definitions and index data:",
            speed: "Speed",
            speedDetails: "<strong>Backup</strong> is usually much faster than a <strong>Snapshot</strong>",
            size: "Size",
            sizeDetails: "<strong>Backup</strong> is usually much smaller than <strong>Snapshot</strong>",
            restore: "Restore",
            restoreDetails: "Restoring a <strong>Snapshot</strong> is usually faster than restoring a <strong>Backup</strong>",
            notes: "Notes:",
            incrementalSnapshot: "An incremental Snapshot is the same as an incremental Backup",
            incrementalSnapshotSharded: "An incremental Snapshot is not supported in sharded databases",
        },
        ageInfo: {
            minimumTime: "Define the minimum time to keep the Backups (and Snapshots) in the system.",
            fullBackupDeleted:
                "A <strong>Full Backup</strong> that is older than the specified retention time will be deleted by RavenDB server.",
            incrementalBackups:
                "If <strong>Incremental Backups</strong> exist, the Full Backup, and its incrementals, are removed only if the <em>last incremental</em> is older than the defined retention time.",
            deletionSchedule: "The deletion occurs when the backup task is triggered on its schedule.",
        },
        ftpHostInfo: {
            protocol:
                "To specify the server protocol, prepend the host with protocol identifier (ftp and ftps are supported).",
            defaultProtocol: "If no protocol is specified the default one (ftp://) will be used.",
            completeUrl: "You can also enter a complete URL",
            example: "e.g. <strong>{{url}}</strong>",
        },
        serverWideSnapshotEncryption: {
            intro: "When selecting the <strong>'Snapshot'</strong> backup-type, encryption is dependant on the database the task is operating on.",
            encrypted:
                "If the database backed up is <strong>encrypted</strong> - the Snapshot will also be encrypted (using the database key).",
            notEncrypted:
                "If the database backed up is <strong>not-encrypted</strong> - the Snapshot will not be encrytped as well.",
        },
        targetOperation: "Backup",
        storageCreatedManually: "{{storageName}} should be created manually in order for this {{targetOperation}} to work.",
        testCredentials: "You can use the 'Test credentials' button to verify its existance.",
        bucketNamingGuidelines: "Bucket naming guidelines",
    },
    externalScriptNotAllowedForNonClusterAdmins:
        "Setting up the configuration via an external script is not allowed for non cluster admins.",
    etlTaskNameLocked:
        "ETL task name cannot be changed after creation. Delete and recreate the task to use a different name.",
    taskNameLocked: "Task name cannot be changed after creation. Delete and recreate the task to use a different name.",
} as const;
