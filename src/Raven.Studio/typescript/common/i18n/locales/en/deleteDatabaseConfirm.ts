export default {
    title_one: "You're deleting {{count}} database",
    title_other: "You're deleting {{count}} databases",
    encrypted: "This database is encrypted",
    notEncrypted: "This database is not encrypted",
    selectOption: "Select a delete option below",
    appliesToAll: "The option selected will apply to all of the deleted databases",
    deletedFromAllNodes: "Database will be deleted from all nodes in the cluster.",
    keepFiles: {
        title: "Delete and keep files",
        filesUntouched: "The physical files will be left untouched on disk.",
        exportAdvice: "Please export this data before deleting if needed.",
        dataNotRecovered:
            "<i>Database Configuration, Identities &amp; Compare Exchange</i> data will not be recovered if the database is to be added again in the future. {{exportAdvice}}",
        secretKeyDeleted_one:
            "<i>The Secret Key</i> will be deleted for the encrypted database.<br/>{{exportKey}}<br/><code>./rvn offline-operation get-key &lt;path-to-database-dir&gt;</code>",
        secretKeyDeleted_other:
            "<i>The Secret Keys</i> will be deleted for the encrypted databases.<br/>{{exportKey}}<br/><code>./rvn offline-operation get-key &lt;path-to-database-dir&gt;</code>",
        exportKeyWithRvn: "In order to export it, please use the rvn tool:",
        exportButton: "Export Data",
        exportButtonTitle: "Navigate to Export Database View",
        keepFilesButton: "Delete & Keep Files",
        keepFilesButtonTitle: "Delete & keep physical database files on disk",
    },
    deleteAll: {
        title: "Delete all data",
        filesDeleted: "All on-disk files will be deleted.",
        cannotBeUndone: "This action cannot be undone.",
        button: "Delete All Data",
        buttonTitle: "Delete all the on-disk files",
    },
    manageGroup: {
        singleNodeHint: "In order to delete the database from only a single node (or multiple nodes) go to",
        link: "Manage Database Group",
        linkTitle: "Navigate to Manage Database Group View",
    },
    cancelTitle: "Cancel the delete operation",
} as const;
