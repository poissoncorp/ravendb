export default {
    bucket: "Bucket",
    bucketInfo: {
        storageName: "Bucket",
    },
    remoteFolderName: "Remote folder name",
    credentialsJson: "Google Credentials Json",
    credentialsPlaceholder: "e.g.\n{{example}}",
    viewCredentialsTitle: "View credentials",
    showCredentials: "Show credentials",
    hideCredentials: "Hide credentials",
    validation: {
        bucketNameLength:
            "Bucket name must contain 3 to 63 characters.Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters",
        allowedCharacters:
            "Bucket name must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.)",
        bucketNameStartEnd: "Bucket name must start and end with a number or letter",
        adjacentSpecialCharacters: "Dashes, periods and underscores are not permitted to be adjacent to another",
        missingType: "Google credentials json is missing 'type' field",
        missingPrivateKey: "Google credentials json is missing 'private_key' field",
        missingClientEmail: "Google credentials json is missing 'client_email' field",
    },
} as const;
