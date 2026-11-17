export default {
    title: "Conflict Resolution",
    loadError: "Unable to load conflict resolution",
    finishEditingHint: "Please finish editing all scripts before saving",
    addScriptButton: "Add new",
    addScriptTitle: "Add a new Conflicts Resolution script",
    scriptsHeading: "Collection-specific scripts",
    noScripts: "No scripts have been defined",
    resolveToLatestSwitch: "If no script was defined for a collection, resolve the conflict using the latest version",
    script: {
        collectionNameFallback: "Collection name",
        notSaved: "The script has not been saved yet",
        lastModified: "Last modified",
        newMark: "(new)",
        collectionLabel: "Collection",
        collectionPlaceholder: "Select collection (or enter a new one)",
        scriptLabel: "Script",
        syntaxButton: "Syntax",
        submit_add: "Add",
        submit_update: "Update",
        submitTitle_add: "Add Script",
        submitTitle_update: "Update Script",
        discard: "Discard",
        discardTitle: "Discard changes",
        editTitle: "Edit this script",
        deleteTitle: "Delete this script",
        hideTitle: "Hide this script",
        showTitle: "Show this script",
        nameUsed: "This collection name is already used",
    },
    syntax: {
        conflictedDocuments: "Conflicted documents",
        script: "Script",
        result: "Conflict resolution result",
    },
    about: {
        intro: "Since RavenDB accepts writes on any node in the cluster,<br/> a conflict may occur when the same document is updated concurrently on two different nodes.",
        behavior:
            "In this view, you can <strong>define the server's behavior</strong> upon a conflict between documents.<br/>When a conflict is detected, the server will attempt to resolve it via the following flow:",
        scriptItem:
            "If a resolving script (a JavaScript function) is defined per the collection,<br/> it will be used to resolve the conflict.<br/> See the available script variables and examples in the provided documentation link.",
        noScriptItem: "Else, when no script is defined:",
        latestOnItem:
            'If the "<strong>Resolve to latest version</strong>" toggle is turned ON:<br/>The server will resolve the conflict to the latest document version.',
        latestOffItem:
            "If the toggle is OFF:<br/> The conflict is not automatically resolved.<br/> You can resolve the conflict manually from the <a>Conflicts view</a> in the Studio.",
        docsLink: "Docs - Conflict Resolution",
    },
} as const;
