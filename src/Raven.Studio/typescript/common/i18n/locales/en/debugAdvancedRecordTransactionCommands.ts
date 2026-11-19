export default {
    title: "Record Transaction Commands",
    warning:
        "WARNING! Unless you know what you are doing, recording transaction commands <strong>might affect server performance!</strong>",
    databaseName: "Database Name",
    selectDatabase: "Select database",
    outputFilePath: "Output File Path",
    outputFilePathPlaceholder: "Enter full file path for saving the recorded output",
    startRecording: "Start recording",
    recordings: {
        title: "Transaction commands recordings:",
        status: "Status",
        database: "Database",
        completed: "Completed",
        running: "Running",
        stop: "Stop",
    },
} as const;
