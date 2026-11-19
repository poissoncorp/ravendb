export default {
    close: "Close",
    copySampleTitle: "Copy code sample",
    sampleCopied: "Sample has been copied to clipboard",
    raven: {
        title: "Sample RavenDB ETL Transformation Scripts",
        simple: "Simple Transformation Script",
        counters: "Transformation Script with custom logic for counters",
        deletions: "Transformation Script with custom logic for documents deletion",
    },
    sql: {
        title: "Sample SQL ETL Transformation Script",
    },
    snowflake: {
        title: "Sample Snowflake ETL Transformation Script",
    },
    elasticsearch: {
        title: "Sample Elasticsearch ETL Transformation Script",
    },
    olap: {
        title: "Sample OLAP ETL Transformation Scripts",
        withPartitions: "Transform with Partitions",
        withoutPartitions: "Transform without Partitions",
        partitionWithKey: "Partition with Key",
        customPartition: "Partition with Custom Partition",
    },
    kafka: {
        title: "Sample Kafka ETL Transformation Script",
    },
    rabbitMq: {
        title: "Sample RabbitMQ ETL Transformation Script",
        example: "Example",
        loadToOption: "<em>loadTo</em> Option",
        targetExchange: "Target Exchange",
        routingKey: "Routing Key",
        emptyDefault: "empty (default)",
    },
    azureQueueStorage: {
        title: "Sample Azure Queue Storage ETL Transformation Script",
    },
    amazonSqs: {
        title: "Sample Amazon SQS ETL Transformation Script",
    },
    embeddings: {
        title: "Sample Embeddings Generation Script",
        text: "Text",
    },
} as const;
