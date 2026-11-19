export default {
    notAvailable: "n/a",
    bucket: {
        pendingRemovalTooltip:
            "This bucket is being moved to shard #{{ownerShard}}. The copy on this shard will be removed once resharding completes.",
    },
    ioStats: {
        indexesTrack: "Indexes",
        invalidFileFormat: "Invalid IO Stats file format",
        parseFailed: "Failed to parse json data",
        clearConfirm: {
            title: "Clear graph data",
            message: "Do you want to discard all collected IO statistics?",
        },
        meterType: {
            journalWrite: "Journal Write",
            dataFlush: "Voron Data Flush",
            dataSync: "Voron Data Sync",
            compression: "Compression Ratio",
        },
        tooltip: {
            gapStartTime: "Gap start time:",
            gapDuration: "Gap duration:",
            duration: "Duration:",
            size: "Size:",
            sizeInBytes: "Size (bytes):",
            allocatedSize: "Allocated Size:",
            allocatedSizeInBytes: "Allocated Size (bytes):",
            speed: "Speed:",
            originalSize: "Original Size:",
            originalSizeInBytes: "Original Size (bytes):",
            compressedSize: "Compressed Size:",
            compressedSizeInBytes: "Compressed Size (bytes):",
            compressionRatio: "Compression Ratio:",
            compressionSpeed: "Compression speed:",
            unknownSpeed: "unknown",
        },
    },
} as const;
