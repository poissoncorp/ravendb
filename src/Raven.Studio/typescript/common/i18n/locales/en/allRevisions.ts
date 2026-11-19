export default {
    revisionsItemsName: "revisions",
    removeButton: "Remove revisions",
    removeSelectedButton_one: "Remove {{count}} revisions",
    removeSelectedButton_other: "Remove {{count}} revisions",
    deleteConfirm: {
        title_one: "Delete selected <strong>({{count}})</strong> revisions?",
        title_other: "Delete selected <strong>({{count}})</strong> revisions?",
        message: "Please be aware that this action is irreversible. <br/>Revisions that are removed cannot be recovered.",
    },
    removeSuccess_one: "Successfully removed {{count}} revisions",
    removeSuccess_other: "Successfully removed {{count}} revisions",
    filters: {
        collectionLabel: "Filter by collection",
        collectionPlaceholder: "Select collection",
        typeLabel: "Filter by type",
        types: {
            all: "All",
            regular: "Regular",
            deleted: "Deleted",
        },
        typePopovers: {
            all: "Display all revisions.",
            regular: "Display only revisions created by document creation or modification.",
            deleted: 'Display only "Delete Revisions" created by document deletion.',
        },
    },
    pagination: {
        label: "Pagination",
        title: "Show the revisions page by page instead of scrolling",
    },
    smallSampleWarning_one:
        'The table displays only part of the results. When both a collection and a type other than "All" are selected, only the first {{count}} results are visible.',
    smallSampleWarning_other:
        'The table displays only part of the results. When both a collection and a type other than "All" are selected, only the first {{count}} results are visible.',
    columns: {
        id: "Id",
        collection: "Collection",
        etag: "Etag",
        lastModified: "Last Modified",
        flags: "Flags",
        shardNumber: "Shard number",
        deleteRevisionPopover:
            'A "Delete Revision" can only be deleted from the<br/><a>Revisions Bin view</a>',
    },
    about: {
        intro: "This view displays ALL revisions for every document in the database.<br/>You can filter the revisions by their <strong>Collection</strong> or <strong>Type</strong>:",
        regularHeading: "<strong>Regular revisions</strong>:",
        regularCreated: "A regular revision is created for a document whenever it is modified.",
        regularDeletable:
            "These revisions can be deleted directly from this view.<br/> Once a revision is removed, it will no longer be available in the database and will Not be listed in this view.",
        deletedHeading: "<strong>Deleted revisions</strong>:",
        deletedCreated:
            'A "Delete Revision" is created for a document when the document is deleted. These revisions are also listed in the Revisions Bin View.',
        deletedDeletableFrom: '"Delete revisions" can be deleted from:',
        revisionsBinItem: "The <a>Revisions Bin View</a>",
        binCleanerItem: "Or set automatic deletion using the <a>Revisions Bin Cleaner</a>",
        caution: "Exercise caution when deleting revisions, as this action cannot be undone.",
    },
} as const;
