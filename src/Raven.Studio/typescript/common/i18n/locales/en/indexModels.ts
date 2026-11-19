export default {
    autoComplete: {
        suggestion: "suggestion",
        methodicalSyntaxNotSupported: "Methodical Syntax Not Supported",
    },
    definition: {
        reduceEmpty: "Reduce function is empty",
        referenceCollectionSameAsOutput:
            "Name for Referencing Collection cannot be the same as the Reduce Output Collection.",
    },
    fieldOptions: {
        yes: "Yes",
        no: "No",
        inherit: "Inherit ({{value}})",
        termVector: {
            no: "No",
            withOffsets: "With offsets",
            withPositions: "With positions",
            withPositionsAndOffsets: "With positions and offsets",
            yes: "Yes",
        },
        indexing: {
            default: "Default",
            no: "No",
            exact: "Exact",
            search: "Search",
            searchImplied: "Search (implied)",
        },
        analyzer: {
            usedWithExact:
                "{{analyzer}} is used when selecting Indexing.Exact. To set a different analyzer, select the 'Indexing.Search' option first.",
            usedWithDefault:
                "{{analyzer}} is used when selecting Indexing.Default. To set a different analyzer, select the 'Indexing.Search' option first.",
            searchImplied: "Cannot edit analyzer when Search is implied",
        },
        indexOrStore:
            "'Indexing' and 'Store' cannot be set to 'No' at the same time. A field must be either Indexed or Stored.",
    },
    spatial: {
        precisionAtEquator_kilometers: "Precision at equator; X: {{x}}, Y: {{y}} kilometers",
        precisionAtEquator_miles: "Precision at equator; X: {{x}}, Y: {{y}} miles",
        precision: "Precision; X: {{x}}, Y: {{y}}",
    },
    vector: {
        invalidDestinationType: "Destination embedding type is invalid for the selected source embedding type.",
        dimensionsSetByEmbedder: "Dimensions are set internally by the embedder.",
        dimensionsPositive: "Number of vector dimensions has to be positive.",
        edgesPositive: "Number of edges has to be positive.",
        candidatesPositive: "Number of candidate nodes has to be positive.",
    },
    testIndex: {
        entryPreview: "Entry preview",
    },
} as const;
