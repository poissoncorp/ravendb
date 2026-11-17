export default {
    title: "Revert Documents to Revisions",
    revertButton: "Revert",
    revertTooltip_all: "Revert documents from all collections to the specified point in time",
    revertTooltip_selected: "Revert documents from the selected collections to the specified point in time",
    confirmTitle: "Do you want to revert documents state to date: {{pointInTime}}?",
    backLink: "Go back to Revisions View",
    backLinkTitle: "Navigate to Document Revisions View",
    pointInTime: {
        label: "Point in Time",
        placeholder: "Select the point in time",
        localAddon: "local",
    },
    timeWindow: {
        label: "Time Window",
        placeholder: "default ({{value}})",
        units: {
            minutes: "minutes",
            hours: "hours",
            days: "days",
        },
    },
    about: {
        intro: "Use this view to revert documents to their revisions as they were at the specified <strong>Point in Time</strong>.<br/>You can choose to revert documents from all collections or only from selected collections.",
        timeWindow:
            "<strong>Time Window</strong> parameter is used for performance optimization: since revisions are not sorted by date, we stop the revert process when hitting a versioned document outside the window.",
        effects:
            "Based on the specified <strong>Point in Time</strong>,<br/>the following will apply when the revert process takes place:",
        modifiedItem:
            "Documents<strong><em> modified </em></strong>after Point in Time:<code> {{pointInTime}} </code>will be reverted (by creating new revision) to latest version before<code> {{pointInTime}} </code>.",
        limitItem:
            "If collection has maximum revisions limit and all of them were<strong><em> created </em></strong>after Point in Time:<code> {{pointInTime}} </code>the oldest revision will be used.",
        createdItem:
            "Documents<strong><em> created </em></strong>after Point in Time:<code> {{pointInTime}} </code>will be moved to<strong><em> Revisions Bin</em></strong>.",
        remainingItem: "Remaining documents will not be modified.",
        docsLink: "Docs - Revert Documents to Revisions",
    },
} as const;
