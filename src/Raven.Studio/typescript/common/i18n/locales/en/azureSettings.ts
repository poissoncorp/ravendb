export default {
    storageContainer: "Storage container",
    storageContainerInfo: {
        storageName: "Storage container",
    },
    remoteFolderName: "Remote folder name",
    accountName: "Account name",
    authentication: "Authentication",
    accountKey: "Account key",
    sasToken: "SAS token",
    viewHiddenCharactersTitle: "View hidden characters",
    hideCharactersTitle: "Hide characters",
    validation: {
        containerNameLength: "Container name should be between 3 and 63 characters long",
        allowedCharacters: "Allowed characters lowercase characters, numbers and dashes",
        containerNameStartEnd: "Container name must start and end with a letter or a number",
        consecutiveDashes: "Consecutive dashes are not permitted in container names",
    },
} as const;
