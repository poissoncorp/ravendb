export default {
    taskType: {
        title: "TASK TYPE",
        externalReplication: "External Replication",
        replicationHub: "Replication Hub",
        replicationSink: "Replication Sink",
        azureQueueStorageEtl: "Azure Queue Storage ETL",
        amazonSqsEtl: "Amazon SQS ETL",
        ai: "AI",
        periodicBackup: "Periodic Backup",
        subscription: "Subscription",
        viewAllTitle: "Navigate to Ongoing Tasks View to see all the database tasks",
        viewAll: "View all Ongoing Tasks",
    },
    nodeType: {
        title: "NODE TYPE",
        member: "Member",
        promotable: "Promotable",
        rehab: "Rehab",
    },
    connectionStatus: {
        title: "CONNECTION STATUS",
        connection: "Connection",
        partial: "Partial Connection (ETL)",
        disabled: "Disabled Connection",
        broken: "Broken Connection",
    },
} as const;
