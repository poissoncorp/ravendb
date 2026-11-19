export default {
    vaultName: "Vault name",
    vaultInfo: {
        storageName: "Vault",
        targetOperationBackup: "Backup",
    },
    remoteFolderName: "Remote folder name",
    region: "Region",
    regionPlaceholder: "Select an AWS region",
    accessKey: "Access key",
    secretKey: "Secret key",
    sessionToken: "Session token",
    viewHiddenCharactersTitle: "View hidden characters",
    hideCharactersTitle: "Hide characters",
    validation: {
        vaultNameLength: "Vault name must be at least 1 character and no more than 255 characters long",
        allowedCharacters: "Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (hyphen), and '.' (period)",
    },
} as const;
