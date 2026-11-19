export default {
    attachmentUpload: {
        title: "Attachment upload",
        uploading: "Uploading '{{fileName}}' for document '{{documentId}}'",
        uploaded: "File '{{fileName}}' (size: {{size}}) was uploaded to document '{{documentId}}'",
    },
    bulkInsert: {
        title: "Bulk inserts",
        completed_one: "{{formattedCount}} bulk insert to database {{databaseName}} completed successfully.",
        completed_other: "{{formattedCount}} bulk inserts to database {{databaseName}} completed successfully.",
        itemsInserted_one: "{{formattedCount}} item was inserted.",
        itemsInserted_other: "{{formattedCount}} items were inserted.",
        failed_one: "{{formattedCount}} bulk insert to database {{databaseName}} has been completed with an error.",
        failed_other: "{{formattedCount}} bulk inserts to database {{databaseName}} has been completed with an error.",
    },
    deleteByQuery: {
        title: "Delete by query",
        completed_one: "{{formattedCount}} operation has been completed successfully.",
        completed_other: "{{formattedCount}} operations has been completed successfully.",
        failed_one: "{{formattedCount}} operation has been completed with an error.",
        failed_other: "{{formattedCount}} operations has been completed with an error.",
    },
    updateByQuery: {
        title: "Update by query",
        completed_one: "{{formattedCount}} operation has been completed successfully.",
        completed_other: "{{formattedCount}} operations has been completed successfully.",
        failed_one: "{{formattedCount}} operation has been completed with an error.",
        failed_other: "{{formattedCount}} operations has been completed with an error.",
    },
    abortConfirm: {
        title: "Are you sure?",
        attachmentUpload: "Do you want to abort attachment upload?",
        operation: "Do you want to abort current operation?",
    },
    operationCanceled: "The operation was canceled",
    postpone: {
        oneHour: "1 hour",
        sixHours: "6 hours",
        oneDay: "1 day",
        oneWeek: "1 week",
    },
} as const;
