export default {
    preview: {
        title: "Document: {{id}}",
    },
    timeSeriesConfiguration: {
        nothingDefined: "No rollup policy, retention time or named value are defined.",
        namesNotUnique: "Time Series names must be unique",
    },
    namedValues: {
        valueRequired: "At least one value is required",
        namesNotUnique: "Values names must be unique",
    },
    policy: {
        timeUnits: {
            seconds: "seconds",
            minutes: "minutes",
            hours: "hours",
            days: "days",
            months: "months",
            years: "years",
            custom: "custom",
        },
        nameNotUnique: "Policy name must be unique.",
        minimumValue: "Minimum value for 'seconds' or 'months' time units is 1",
        aggregationNotPositive: "Aggregation time must be greater than zero",
        wholeNumber: "Please use a whole number for 'seconds' or 'months' time units",
        aggregationNotGreaterThanPrevious: "Time frame must be greater than the preceding aggregation time ({{time}})",
        aggregationNotLessThanPreviousRetention: "Time frame must be less than the preceding retention time ({{time}})",
        aggregationNotMultiple:
            "Time value must be divided by the previous policy aggregation time without a remainder",
        retentionNotPositive: "Retention time must be greater than zero",
    },
    timeValue: {
        seconds_one: "{{formattedCount}} second",
        seconds_other: "{{formattedCount}} seconds",
        minutes_one: "{{formattedCount}} minute",
        minutes_other: "{{formattedCount}} minutes",
        hours_one: "{{formattedCount}} hour",
        hours_other: "{{formattedCount}} hours",
        days_one: "{{formattedCount}} day",
        days_other: "{{formattedCount}} days",
        months_one: "{{formattedCount}} month",
        months_other: "{{formattedCount}} months",
        years_one: "{{formattedCount}} year",
        years_other: "{{formattedCount}} years",
    },
} as const;
