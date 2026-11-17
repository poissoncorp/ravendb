export default {
    title: "Revisions Bin Cleaner",
    loadError: "Unable to load revisions bin cleaner",
    enableSwitch: "Enable Revisions Bin Cleaner",
    minimumAgeSwitch: "Set minimum entries age to keep",
    minimumAgeOffWarning:
        "All items in the Revisions Bin will be deleted when 'Set minimum entries age to keep' is toggled off.",
    customFrequencySwitch: "Set custom cleaner frequency",
    defaultPlaceholder: "Default ({{value}})",
    secondsAddon: "seconds",
    about: {
        automaticDeletion:
            '<strong>Automatic deletion</strong>:<br/>This view allows you to configure automatic deletion of <strong><code>"Delete Revisions"</code></strong> that are stored in the <a>Revisions Bin</a>. You can set the following:',
        minimumAge:
            "<strong>Minimum age</strong><br/>Revisions that have been in the Revisions Bin for longer than this duration will be removed when the cleaner executes.",
        cleanerFrequency:
            "<strong>Cleaner frequency</strong><br/>Defines how often the cleaner executes to remove revisions.",
        manualDeletion:
            '<strong>Manual deletion</strong>:<br/>Manual deletion of individual "Delete Revisions" can be performed from the <a>Revisions Bin View</a>.',
    },
} as const;
