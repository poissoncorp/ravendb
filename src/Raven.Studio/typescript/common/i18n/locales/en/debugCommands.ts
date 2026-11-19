export default {
    tombstonesCleanup: {
        nothingToCleanup: "No tombstones to cleanup",
        success_one: "Successfully cleaned up {{formattedCount}} tombstone",
        success_other: "Successfully cleaned up {{formattedCount}} tombstones",
        failed: "Failed to cleanup tombstones",
    },
    loadBucketFailed: "Failed to load bucket",
    loadBucketsReportFailed: "Failed to load buckets report",
    loadMemoryStatsFailed: "Failed to load memory statistics",
    loadThreadsRuntimeInfoFailed: "Failed to load Threads Runtime Info",
    loadDetailedStorageReportFailed: "Failed to load detailed storage report",
    loadStorageReportFailed: "Failed to load storage report",
    loadTombstonesStateFailed: "Failed to get tombstones state",
    promoteNodeFailed: "Failed to promote node {{nodeTag}}",
    transactionsRecording: {
        started: "Transaction Commands Recoding was started for database: {{databaseName}}",
        startFailed: "Failed to start recording transaction commands",
        stopped: "Transaction Commands Recoding was stopped",
        stopFailed: "Failed to stop recording transaction commands",
    },
    replayTransactionsFailed: "Failed to Replay Transaction Commands",
} as const;
