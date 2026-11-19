export default {
    entry: {
        title_new: "New Time Series Entry",
        title_edit: "Edit Time Series Entry",
        title_newIncremental: "New Time Series Entry - Incremental",
        title_editIncremental: "Edit Time Series Entry - Incremental",
        title_newRollup: "New Time Series Entry - Rollup",
        title_editRollup: "Edit Time Series Entry - Rollup",
        title_newIncrementalRollup: "New Time Series Entry - Incremental - Rollup",
        title_editIncrementalRollup: "Edit Time Series Entry - Incremental - Rollup",
        maxRollupValuesReached: "The maximum number of possible rollup values ({{limit}}) has been reached.",
        maxValuesReached: "The maximum number of possible values ({{limit}}) has been reached.",
    },
    validation: {
        nameContainsAt: "A Time Series name cannot contain '@'. This character is reserved for Time Series Rollups.",
        incrementalPrefixRequired: "An Incremental Time Series name must start with prefix: '{{prefix}}'",
        regularPrefixNotAllowed: "A regular Time Series name cannot start with prefix: '{{prefix}}'",
        invalidDate: "Please enter a valid date",
        valueRequired: "At least one value is required",
        rollupValueRequired: "At least one rollup value is required",
    },
} as const;
