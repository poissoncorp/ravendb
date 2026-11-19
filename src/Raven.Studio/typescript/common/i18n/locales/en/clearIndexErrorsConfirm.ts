export default {
    title_all: "Clear errors for ALL indexes ?",
    title_single: "Clear index Errors?",
    title_multiple: "Clear indexes Errors?",
    subTitle_all: "Errors will be cleared for ALL indexes",
    subTitle_single: "You're clearing errors from this index:",
    subTitleMultiple_one: "You're clearing errors for <strong>{{count}}</strong> indexes:",
    subTitleMultiple_other: "You're clearing errors for <strong>{{count}}</strong> indexes:",
    errorStateInfo:
        "While the current indexing errors will be cleared,<br />an index with an 'Error state' will Not be set back to 'Normal state'.",
    clearButton: "Clear Index Errors",
    clearSucceeded: "Done clearing indexing errors.",
    clearFailed: "Failed to clean indexes errors ({{failed}} / {{total}} failed)",
} as const;
