export default {
    errorMessage: "{{row}},{{column}}: error: {{text}}",
    loadSampleConfirm: {
        title: "Load sample into the editor?",
        message: "The current editor content will be replaced.",
        confirm: "Load",
    },
    fullScreenHint: "Press Shift+F11 to enter full screen mode",
    samplesPlaceholder: "// Start writing, or <button>browse samples</button>",
    actions: {
        browseSamples: "Browse samples",
        resizeToContent: "Resize to content",
        format: "Format",
        fullScreen: "Full screen",
        syntaxHelp: "Syntax help",
        loadFromFile: "Load from a file",
        toggleNewLines: "Toggle new lines",
        invalidJson: "The document data isn't a legal JSON expression!",
    },
} as const;
