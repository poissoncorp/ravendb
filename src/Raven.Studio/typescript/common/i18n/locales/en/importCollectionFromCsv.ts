export default {
    sharded: {
        featureNotAvailable: "Feature not available",
        notAvailable: "Import documents from a CSV file into a collection is not available for <icon/> sharded databases",
    },
    title: "Import documents from a CSV file into a collection",
    source: {
        title: "Define source and destintaion",
        importFromFile: "Import from File",
        selectFile: "Select file...",
        browse: "Browse",
        headersRequired: "CSV file must contain headers.",
        columnsAsProperties: "Each column will be treated as a property.",
        importToCollection: "Import to Collection",
        collectionPlaceholder: "Enter collection name (only if doesn't exist in file)",
        collectionFromColumn:
            "The Collection name will be derived from the CSV file column <strong>@metadata.@collection</strong> - if it exists.",
        collectionFromInput: "Else, you can enter a collection name here.",
        collectionFromFileName: "If not specified, then the collection name will be derived from the CSV file name.",
    },
    csvOptions: {
        title: "CSV options",
        fieldsDelimiter: "Fields Delimiter",
        delimiters: {
            comma: "Comma",
            semicolon: "Semicolon",
            tab: "Tab",
            space: "Space",
        },
        textQualifier: "Text Qualifier",
        quoteChars: {
            doubleQuote: "Double quote",
            singleQuote: "Single quote",
        },
        whitespace: "Whitespace",
        trimOptions: {
            noTrimming: "No trimming",
            trimAroundFields: "Trim whitespace around fields",
            trimInsideStrings: "Trim whitespace inside strings",
        },
        allowComments: "Allow Comments",
        commentCharacterPlaceholder: "Enter comment character",
        commentCharacterInvalid: "Please enter only one character for the comment character",
    },
    importCollection: "Import Collection",
    nextOperationIdFailed: "Could not get next task id.",
} as const;
