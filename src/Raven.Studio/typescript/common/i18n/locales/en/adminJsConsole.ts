export default {
    title: "Admin JS Console",
    warning: {
        title: "Warning",
        message:
            "Do not use the console unless you are sure about what you're doing. Running a script in the Admin Console could cause your server to crash, cause loss of data, or other irreversible harm.",
    },
    scriptTarget: "Script target",
    targets: {
        server: "Server",
        orchestrator: "{{name}} (orchestrator)",
    },
    accessibleVariable: "Accessible within the script under <code>{{variable}}</code> variable",
    script: "Script",
    scriptResult: "Script result",
    about: {
        intro: "<strong>Admin JS Console</strong> is a specialized feature primarily intended for resolving server errors. It provides a direct interface to the underlying system, granting the capacity to execute scripts for intricate server operations.",
        usage: "It is predominantly intended for advanced troubleshooting and rectification procedures executed by system administrators or RavenDB support.",
        docsLink: "Docs - Admin JS Console",
    },
} as const;
