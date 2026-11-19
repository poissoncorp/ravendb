export default {
    responsibleNode: {
        preferredNodeTitle: "User preferred node for this task",
        temporaryNodeTitle: "Cluster node that is temporary responsible for this task",
        hubNodeTitle: "Hub node that is serving this Sink task",
        clusterNodeTitle: "Cluster node that is responsible for this task",
        noNodeTitle: "No node is currently handling this task",
        notAvailable: "N/A",
    },
    taskNameTitle: "Task name: {{name}}",
    status: {
        taskState_Enabled: "Enabled",
        taskState_Disabled: "Disabled",
        taskState_PartiallyEnabled: "PartiallyEnabled",
        taskState_None: "None",
        enable: "Enable",
        disable: "Disable",
    },
    actions: {
        detailsTitle: "Click for details",
        editTitle: "Edit task",
        deleteTitle: "Delete task",
    },
    connectionString: {
        label: "Connection String",
        nameTitle: "Connection string name",
        notDefined: "This connection string is not defined.",
    },
    destinationUrl: "Destination URL",
    replicationMode: {
        both: "Hub to Sink & Sink to Hub",
        hubToSink: "Hub to Sink",
        sinkToHub: "Sink to Hub",
    },
    emptyScriptsWarning: "Following scripts don't match any documents: {{scripts}}",
    operationConfirm: {
        title_enable: "You're about to <strong>enable</strong> following tasks",
        title_disable: "You're about to <strong>disable</strong> following tasks",
        title_delete: "You're about to <strong>delete</strong> following tasks",
        skipping_enable: "Skipping already enabled tasks",
        skipping_disable: "Skipping already disabled tasks",
        disablingWarning_one:
            "Please note, <strong>disabling</strong> this task will lead to continuous tombstone accumulation until it is re-enabled or deleted, resulting in increased disk space usage.",
        disablingWarning_other:
            "Please note, <strong>disabling</strong> these tasks will lead to continuous tombstone accumulation until they are re-enabled or deleted, resulting in increased disk space usage.",
        disablingWarningWithSubscriptions:
            "Please note, <strong>disabling</strong> the selected tasks (excluding <strong>Subscription</strong> tasks) will lead to continuous tombstone accumulation until they are re-enabled or deleted, which will increase disk space usage. This warning does not apply to Subscription tasks.",
        submit_enable: "Enable",
        submit_disable: "Disable",
        submit_delete: "Delete",
    },
    operations: {
        toggleSuccess_enabled_one: "Task enabled successfully.",
        toggleSuccess_enabled_other: "Tasks enabled successfully.",
        toggleSuccess_disabled_one: "Task disabled successfully.",
        toggleSuccess_disabled_other: "Tasks disabled successfully.",
        deleteSuccess_one: "Task deleted successfully.",
        deleteSuccess_other: "Tasks deleted successfully.",
    },
    addTask: {
        noMatch: "No tasks match your filter criteria",
        subscriptionLimit_cluster: "Cluster has reached the maximum number of subscriptions allowed per cluster.",
        subscriptionLimit_database: "Database has reached the maximum number of subscriptions allowed per database.",
        shardingNotSupported: "Sharding is not supported for this task",
        categories: {
            ai: "AI",
            replication: "Replication",
            backups: "Backups",
            subscriptions: "Subscriptions",
            etlHeader: "ETL (RavenDB ⇛ TARGET)",
            sink: "Sink",
            sinkHeader: "Sink (SOURCE ⇛ RavenDB)",
        },
        tasks: {
            genAi: {
                title: "GenAI",
                description: "Analyze and enrich your documents using an LLM.",
            },
            embeddingsGeneration: {
                title: "Embeddings Generation",
                description: "Automatically generate embeddings from your document content.",
            },
            externalReplication: {
                title: "External Replication",
                description: "Create a live replica of your database in another RavenDB database in another cluster.",
            },
            replicationHub: {
                title: "Replication Hub",
                description:
                    "Replicate documents to and/or from multiple Replication Sink tasks in other RavenDB databases across different clusters.",
            },
            replicationSink: {
                title: "Replication Sink",
                description:
                    "Connect to a central Replication Hub in another RavenDB cluster to receive documents, and optionally replicate back.",
            },
            periodicBackup: {
                title: "Periodic Backup",
                description: "Create periodic backups or snapshots of the database on a defined schedule.",
            },
            subscription: {
                title: "Subscription",
                description: "Send batches of documents that match a pre-defined query to a client for processing.",
            },
            ravenEtl: {
                description:
                    "Extract and transform selected database documents and write them to another RavenDB database.",
            },
            elasticSearchEtl: {
                description:
                    "Extract and transform data from selected documents and transfer it to an Elasticsearch destination.",
            },
            kafkaEtl: {
                description: "Extract and transform data from selected documents and send it to Kafka topics.",
            },
            sqlEtl: {
                description: "Extract and transform data from selected documents and write it to a relational database.",
            },
            snowflakeEtl: {
                description: "Extract and transform data from selected documents and write it to a Snowflake database.",
            },
            olapEtl: {
                description:
                    "Extract and transform data from selected documents and export it as Parquet files to the specified destination.",
            },
            rabbitMqEtl: {
                description: "Extract and transform data from selected documents and send it to a RabbitMQ exchange.",
            },
            azureQueueStorageEtl: {
                title: "Azure Queue Storage ETL",
                description: "Extract and transform data from selected documents and send it to Azure Queue Storage.",
            },
            amazonSqsEtl: {
                title: "Amazon SQS ETL",
                description: "Extract and transform data from selected documents and send it to Amazon SQS queues.",
            },
            kafkaSink: {
                title: "Kafka Sink",
                description:
                    "Consume and process incoming JSON messages from Kafka topics to create or delete documents.",
            },
            rabbitMqSink: {
                title: "RabbitMQ Sink",
                description:
                    "Consume and process incoming JSON messages from RabbitMQ queues to create or delete documents.",
            },
            azureServiceBusSink: {
                title: "Azure Service Bus Sink",
                description:
                    "Consume and process incoming JSON messages from Azure Service Bus queues to create or delete documents.",
            },
            cdcSink: {
                title: "CDC Sink",
                description:
                    "Consume Change Data Capture streams from relational databases and apply inserts, updates, and deletes to documents in RavenDB.",
            },
        },
    },
} as const;
