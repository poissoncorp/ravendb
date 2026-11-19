export default {
    about: {
        intro: "You can configure a <strong>GenAI task</strong> to analyze and enrich your documents using <strong>LLMs</strong>.<br/>The task can automatically update existing documents or create new ones based on AI-generated content.",
        configurationIncludes: "Task configuration includes:",
        aiServiceAndModel:
            "<strong>AI service and model:</strong><br/>Select a <a>connection string</a> that defines the AI provider and model to connect to.",
        defineModelInput: "Define MODEL INPUT:",
        contextObjects:
            "<strong>Context objects</strong><br/>Provide a script to create context objects from each document in the source collection. These will be the input objects for the model.",
        prompt: "<strong>Prompt</strong><br/>Define the instruction sent to the model.<br/>It will be applied to each context object.",
        jsonSchema:
            "<strong>JSON schema</strong><br/>Specify the expected format of the output objects the model should generate.<br/>An output object will be returned for each input context object.",
        handleModelOutput:
            '<strong>Handle MODEL OUTPUT:</strong><br/>Provide an "update script" that will process each output object returned by the model.<br/>You can modify existing documents or create new ones based on the generated results, as needed.',
        testing:
            "<strong>Testing:</strong><br/>You can test the task definition at any stage using any source document.<br/>Use the playground to provide custom context objects and/or model output,<br/>and preview the full flow before saving the task.",
        ongoingProcessing:
            "<strong>Ongoing processing:</strong><br/>Once the task is active, any modification to a document in the selected collection<br/>will trigger the task to retrieve content from the model and apply the update script.",
        overviewDocsLink: "Docs - GenAI Task Overview",
        createTaskDocsLink: "Docs - Create GenAI Task",
    },
    featureAvailability: {
        genAi: "GenAI",
    },
    steps: {
        progress_one: "{{count}}/{{total}} steps completed",
        progress_other: "{{count}}/{{total}} steps completed",
        basic: "Configure basic settings",
        context: "Generate context objects",
        modelInput: "Define prompt & JSON schema",
        updateScript: "Provide update script",
        summary: "Review task configuration",
    },
    footer: {
        cancelTitle: "Cancel configuration and return to the tasks list.",
        back: "Back",
        next: "Next",
    },
    basic: {
        title: "Configure GenAI task basic settings",
        intro: "Configure a GenAI task to analyze and enrich your documents using an LLM.<br/>The connection string defined in this step will be used to connect to the selected model.",
        taskName: "Task Name",
        taskNamePlaceholder: "My task",
        identifier: "Identifier",
        identifierInfo:
            "A unique identifier for the task.<br/>Used in the source document metadata to hold an array of unique hashes representing each model request.<br/><br/>If not specified, it will be auto-generated from the task name.",
        regenerateIdentifierTitle: "Click to generate the identifier from the task name",
        regenerateIdentifier: "Regenerate",
        taskState: "Task State",
        stateEnabled: "Enabled",
        stateDisabled: "Disabled",
        encryptedDatabase: "Database <strong>{{databaseName}}</strong> is encrypted",
        allowNonEncryptedChannel: "Allow task on a non-encrypted communication channel",
        maxConcurrency: "Max concurrency",
        maxConcurrencyInfo:
            "The maximum number of concurrent requests sent to the model (default: {{defaultMaxConcurrency}}).<br/>Each request includes: a context object, the prompt, and the JSON schema.",
        useStartingPoint: "Use a defined starting point",
        sendDocumentsFrom: "Send Documents From",
        startingPointBeginningOfTime: "Beginning of Time",
        startingPointLatestDocument: "Latest Document",
        changeVectorPlaceholder: "Enter change vector to start sending documents from",
        nextBatchStartingPoint: "Next Batch Starting Point <small>(read-only)</small>",
        testConnectionInfo: "Test the connection to the specified connection string.",
        selectConnectionToTest: "Select a connection string to test.",
        testConnection: "Test connection",
    },
    context: {
        title: "Generate context objects",
        intro: "The context objects generated in this step will be used as input for the model.<br/>Each context object will be sent in a separate request, along with the prompt and JSON schema defined in the next step.<br/>Use the playground to test the context generation script on a sample document.",
        sourceCollection: "Source collection",
        sourceCollectionInfo: "Select the collection to use as the source of documents for the task.",
        script: "Context generation script",
        scriptInfo:
            "Use <code>ai.genContext</code> in this script to generate a <strong>context object</strong> from the source document.<br/>Each context object will be passed as a separate input to the model.",
        testDisabledNoDocument:
            "To test the context generation script, select or provide a sample document in the playground.",
        testInfo:
            "Click to test the context generation script on the sample document.<br/>The resulting context object(s) will be shown in the results pane.",
        test: "Test context",
        testFailed: "Failed to create context objects",
    },
    model: {
        title: "Define prompt & JSON schema",
        intro: "The prompt and schema defined in this step will be used as input to the model for each context object generated in the previous step.<br/>Use the playground to test the model's output on this input.",
        prompt: "Prompt",
        promptInfo:
            "Define the instruction that will be sent to the model.<br/>It will be applied to each context object generated in the previous step.",
        aiAssistSuccess: "AI refined your prompt based on your input and information from the previous steps.",
        queryToolsTitle: "Define query tools",
        queryToolsInfo:
            "Define queries that the LLM can use to retrieve data from the database.<br/><br/>These queries are executed in the background by an agent. The LLM does not access the database directly.",
        queryTools: "Query tools",
        addQueryTool: "Add new query tool",
        enableTracing: "Enable conversation documents",
        enableTracingInfo:
            "Toggle on to store the internal interaction between the GenAI Task, the LLM, and the agent that runs in the background (including query calls and responses) as a conversation document in the <code>@conversations</code> collection.<br/><br/>The document ID will be prefixed with the GenAI Task ID.",
        setTracingExpiration: "Set conversation documents expiration",
        setTracingExpirationInfo: "Toggle on to set how long conversation documents are retained.",
        testDisabledNoContexts:
            "To test the model output, either generate context objects in the previous step or enter custom ones here using Edit mode.",
        testInfo:
            "Click to test the model output using the combined input: context objects, prompt, and schema.<br/>The resulting output object(s) will be shown in the results pane.",
        test: "Test model",
    },
    queryTool: {
        title: "Configure query tool",
        toolName: "Tool name",
        toolNamePlaceholder: "e.g. GetOrdersByCountryAndCompany",
        description: "Description",
        descriptionPlaceholder:
            "In this description, explain to the LLM when it should trigger this query.\nExample 1: Use this query to retrieve Order documents from the database filtered by destination country and company.\nExample 2: Use this query to perform a semantic search for products similar to those in the customer's order.",
        query: "Query",
        queryInfo:
            'Define a query the LLM can use to retrieve data from the database.<br/><br/>You can filter the query using parameters exposed as properties in the context objects defined in the previous step. If your query uses a parameter that is not defined in the context object, you must declare it manually in the "Sample parameters object" below.',
        testQueryTitle: "Click to test this query in the Studio's Query View",
        testQuery: "Test query",
        queryPlaceholder:
            '// Enter the query that will run against the database. \n// You can query an existing static index or make a dynamic query. For example:\n// Example 1: from "Orders" where ShipTo.Country == $country" and Company == $company"\n// Example 2: from "Products" where vector.search(embedding.text(Name), $searchTerm, $similarityLevel)\n\n// When using a parameter (e.g., $country), if it\'s defined as a property in the context object (from the previous step),\n// its value will be taken from the generated context.\n// Otherwise, you must define the parameter manually in the "Sample parameters object" section below.',
        sampleObjectLabel: "Sample parameters object",
        sampleObjectInfo:
            "Provide a JSON object that defines the parameters the LLM is expected to supply when it requests the agent to execute this query tool.<br/><br/>This object is not sent to the model directly - RavenDB uses it to generate a JSON schema, which is sent to the model.",
        sampleObjectPlaceholder:
            '{\n    // "ParamName": "Instruction to the LLM",\n    // ... \n    // "ParamName" is the name of a parameter from the query for which the LLM needs to provide a value.\n    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what value to supply in this field.\n}\nOpen the (?) icon to view an example.',
        parametersSchemaLabel: "Parameters JSON schema",
        advancedSettings: "Advanced settings",
        allowModelQueries: "Allow model queries",
        allowModelQueriesInfo:
            "When True, the model is allowed to execute this query on demand based on its own judgment.<br/><br/>When False, the model cannot call this query (unless executed as part of initial context).<br/><br/>When Default, server default is applied.",
        selectTrueOrFalse: "Select True or False",
        defaultPlaceholder: "Default",
        trueOption: "True",
        falseOption: "False",
        override: "Override",
        addToInitialContext: "Add to initial context",
        addToInitialContextInfo:
            "When True, the query will be executed during the initial context build and its results provided to the model.<br/><br/>When False, the query will not be executed for the initial context.<br/><br/>When Default, server default is applied.",
    },
    update: {
        title: "Provide update script",
        intro: 'The "update script" provided in this step will be used to modify your source documents based on the content of the model’s output objects.<br/>Use the playground to test the effect of the script on a sample source document.',
        updateScript: "Update script",
        updateScriptInfo:
            "Provide a script to update your source documents using the model's response.<br/>You can refer to <code>$input</code> (the context object) and <code>$output</code> (the model's response object) within the script.",
        testDisabledNoDocument:
            "To test the the update script, select or provide a sample document in the playground.",
        testDisabledNoContexts:
            "To test the update script, either generate context objects in the 'Generate context objects' step or enter custom ones using Edit mode.",
        testDisabledNoModelOutputs:
            "To test the update script, either generate model output objects in the previous step or enter custom ones here using Edit mode.",
        testDisabledCountMismatch:
            "To test the update script, the number of context objects and model output objects must be the same.",
        testInfo:
            "Click to test the update script.<br/>The updated sample source document will be shown in the results pane.",
        test: "Test update script",
    },
    playground: {
        title: "Playground",
        titleInfo:
            "Use the playground to select/enter sample content for testing the outcome of this configuration step.",
        expand: "Expand",
        collapse: "Collapse",
        documentTab: "Document",
        documentTabInactiveInfo:
            "The selected document has no effect on testing in this step.<br/><br/>The test uses the context objects (generated in the previous playground step or customized here), along with the prompt and JSON schema.",
        contextTab: "Context input",
        modelOutputTab: "Model output",
        modelOutputTabInactiveInfo: "This configuration doesn’t give any additional context to the active step.",
        editMode: "Edit mode",
        editModeInfo:
            'When in "Edit mode", you can modify the content in the Playground as you wish.<br/>Be aware that any changes made to the content in this mode will NOT be saved to the original element.',
        editModeConfirm: {
            title: "You’re about to enter Playground edit mode",
            message:
                "While in Playground edit mode, you can modify the selected content as you wish.<br/>Be aware that any changes made to the content in this mode will NOT be saved to the original element.",
            confirm: "Enter edit mode",
        },
        documentInfo:
            "In this playground area, you can select a document in order to test the outcome of the context generation script and view the resulting context object(s).<br/>Choose an existing document or manually enter a new one (Edit mode).",
        selectDocument: "Select a document from the source collection",
        selectDocumentPlaceholder: "E.g. Posts/01",
        enterDocumentManually: "Or enter a document manually",
        selectedDocument: "Selected document: <strong>{{documentId}}</strong>",
        resetSelection: "Reset selection",
        contextInfo:
            "This playground area shows the context objects generated in the previous step.<br/>Alternatively, you can enter custom context objects manually using Edit mode.<br/>You can then test the model’s response based on the combined input: the context objects, and the prompt and schema defined in this step.",
        forceReprocess: "Force reprocess",
        forceReprocessInfo:
            'When enabled, clicking "Test model" will send a test request to the model even if the sample document and context objects (from the playground), and the prompt and JSON schema (from the task definition) haven’t changed.',
        addContextObject: "Add new context object",
        modelOutputInfo:
            'This playground area shows the model output objects generated in the previous step.<br/>Alternatively, you can enter custom output objects manually using Edit mode.<br/>You can then test to see how the "update script" would affect the document that was selected in the first playground step.',
        addOutputObject: "Add new output object",
    },
    testResults: {
        contextTitle: "Context test results",
        rerun: "Re-run test",
        close: "Close",
        noResults: "No results",
        modelTitle: "Model test results",
        tokensUsed: "Tokens used: {{tokens}}",
        updateScriptTitle: "Update script test results",
        testResult: "Test result",
        splitView: "Split view",
        originalDocument: "Original document",
        modifiedDocument: "Modified document",
    },
    objectList: {
        empty: "Empty list",
        contextObjectId: "Context object ID",
        modelOutputObjectId: "Model output object ID",
        modelOutputCached:
            'Model output is cached. You can <button>enable "Force reprocess"</button> and test again to see the results.',
    },
    reasoning: {
        title: "Reasoning",
        role_system: "Role: system",
        role_user: "Role: user",
        role_assistant: "Role: assistant",
        toolCall: "Tool call: {{name}}",
        parametersFilledByLlm: "Parameters filled by LLM",
        results_one: "Results: {{count}}",
        results_other: "Results: {{count}}",
    },
    attachments: {
        title: "Attachments",
        seeAttachments_one: "See attachments ({{formattedCount}})",
        seeAttachments_other: "See attachments ({{formattedCount}})",
        someNotFound: "Some attachments could not be found.",
        columns: {
            name: "Name",
            source: "Source",
            type: "Type",
            data: "Data",
        },
    },
    summary: {
        title: "Review task configuration",
        intro: "This step summarizes your task configuration.<br/>Please review all settings before saving the task.",
        edit: "Edit",
        basicSettings: "Basic settings",
        taskName: "Task name",
        taskState: "Task state",
        responsibleNode: "Responsible node",
        connectionString: "Connection string",
        maxConcurrency: "Max concurrency",
        contextInput: "Context input",
        sourceCollection: "Source collection",
        contextGenerationScript: "Context generation script",
        promptAndSchema: "Prompt & schema",
        prompt: "Prompt",
        sampleObject: "Sample object",
        jsonSchema: "JSON schema",
        queryTools: "Query tools",
        conversationDocuments: "Conversation documents",
        enabled: "Enabled",
        disabled: "Disabled",
        conversationDocumentsExpiration: "Conversation documents expiration",
        documentUpdate: "Document update",
        updateScript: "Update script",
        reprocessAll: "Reprocess all documents",
        reprocessAllInfo:
            "If enabled, the task will attempt to reprocess all documents from the source collection.<br/><br/>Note: Documents will not be reprocessed if the hash in their metadata exactly matches the hash generated from the current task configuration.",
        back: "Back",
        show: "Show",
        collapse: "Collapse",
    },
    samples: {
        samplePromptTab: "Sample prompt",
        sampleObjectTab: "Sample object",
        sampleSchemaTab: "Sample schema",
        contextScript: {
            generateTitle: "Generate context objects",
            generateDescription: "Iterate over the source document and emit one context object per item.",
            attachmentsTitle: "With attachments",
            attachmentsDescription: "Attach document attachments (images, text files) to the generated context objects.",
        },
        prompt: {
            spamDetectionTitle: "Spam detection prompt",
        },
        sampleObject: {
            responseTitle: "Sample response object",
            responseDescription: "The shape of the object the model should return.",
        },
        updateScript: {
            updateSourceTitle: "Update the source document",
            updateSourceDescription:
                "React to the model output: remove spam comments and archive them in a separate collection.",
        },
        jsonSchema: {
            title: "Sample JSON schema",
            description: "The schema that will be sent to the model, describing the expected response object.",
        },
        queryParameters: {
            queryTitle: "Query parameters",
            queryDescription: "Instruct the LLM what value to supply for each query parameter.",
            vectorSearchTitle: "Vector search parameters",
            vectorSearchDescription: "Parameters for a semantic search query.",
            noParametersTitle: "No parameters",
            noParametersDescription: "Use an empty object when the query takes no parameters.",
        },
        queryParametersSchema: {
            title: "Parameters JSON schema",
            description: "The schema generated from the sample parameters object.",
        },
    },
    methods: {
        contextObjects: {
            category: "Context objects",
            genContext: "Emits one context object, built from the <code>ctx</code> object you pass in. Each context object is sent to the model as a separate request, so call this once per item you want the model to reason about. <code>ctx</code> must be a plain object; do not pass <code>null</code> or an array. Returns the context item, so attachments can be chained onto it. <strong>Context generation script only.</strong>",
            withText: "Attaches <code>data</code> to the context object as <code>text/plain</code>. Returns the context item, so calls can be chained. <code>data</code> must be a string or <code>null</code>. <strong>Context generation script only.</strong>",
            withPng: "Attaches <code>data</code> as <code>image/png</code>. Use an attachment reference returned by <code>loadAttachment()</code>, a Base64-encoded string, or <code>null</code>. Use it to send an image to a vision-capable model. <strong>Context generation script only.</strong>",
            withJpeg: "Attaches <code>data</code> as <code>image/jpeg</code>. A string supplied directly must be Base64-encoded. <strong>Context generation script only.</strong>",
            withWebp: "Attaches <code>data</code> as <code>image/webp</code>. A string supplied directly must be Base64-encoded. <strong>Context generation script only.</strong>",
            withGif: "Attaches <code>data</code> as <code>image/gif</code>. A string supplied directly must be Base64-encoded. <strong>Context generation script only.</strong>",
            withPdf: "Attaches <code>data</code> as <code>application/pdf</code>. A string supplied directly must be Base64-encoded. Useful for sending a whole document to the model instead of extracting its text first. <strong>Context generation script only.</strong>",
        },
        scriptInput: {
            category: "Script input",
            sourceDocument: "The source document. In the context generation script it is the document the context objects are built from; in the update script it is the document being modified, and any change you make to it is persisted.",
            input: "The context object that produced this model response — exactly what was passed to <code>ai.genContext()</code>. Use it to correlate the response back to the part of the document it came from. <strong>Update script only.</strong>",
            output: "The model response, already parsed into an object matching the JSON schema defined in the previous step. <strong>Update script only.</strong>",
        },
        attachments: {
            category: "Source document attachments",
            loadAttachment: "Loads an attachment of the source document so it can be passed to one of the <code>AIContextItem</code> attachment methods. In a GenAI task a missing attachment resolves to an empty attachment rather than throwing, so guard with <code>hasAttachment()</code> when the attachment is optional. <strong>Context generation script only.</strong>",
            hasAttachment: "Returns whether the source document has an attachment with this name. The comparison is case-insensitive. <strong>Context generation script only.</strong>",
            getAttachments: "Returns the source document's attachment metadata — <code>Name</code>, <code>ContentType</code>, <code>Hash</code>, <code>Size</code> — or an empty array when there are none. Takes no arguments. <strong>Context generation script only.</strong>",
            getRevisionsCount: "Returns how many revisions the source document has. Takes no arguments. <strong>Context generation script only.</strong>",
        },
        documents: {
            category: "Document operations",
            id: "Returns the ID of a document object. Use <code>id(this)</code> for the source document, or pass any document variable available in the script, such as a document returned by <code>load()</code>.",
            getMetadata: "Returns the document's metadata, e.g. <code>@id</code>, <code>@collection</code>, <code>@last-modified</code>.",
            lastModified: "Returns the document's last modification time as JavaScript milliseconds since the Unix epoch (UTC).",
            load: "Loads a related document by ID so you can include its fields. Pass an array of IDs to load several at once. Returns <code>undefined</code> when the document does not exist.",
            loadPath: "Loads the document (or documents) referenced by a path inside another document, e.g. <code>\"Comments[].AuthorId\"</code>.",
            cmpxchg: "Returns the value stored under a compare-exchange key, or <code>null</code>.",
            put: "Creates or overwrites a document and returns its ID. Pass an ID ending in <code>/</code> to get a server-generated identifier. <strong>Update script only</strong> — the context generation script runs read-only and this throws <em>\"Cannot make modifications in readonly context\"</em>.",
            del: "Deletes a document and returns whether it existed. <strong>Update script only</strong> — the context generation script runs read-only and this throws <em>\"Cannot make modifications in readonly context\"</em>.",
            archiveAt: "Schedules the document to be archived at the given UTC time. <strong>Update script only.</strong>",
        },
        counters: {
            category: "Counter operations (read-only in GenAI scripts)",
            getCounters: "Returns the names of the source document's counters, read from its metadata, or an empty array when there are none. Takes no arguments. <strong>Context generation script only.</strong>",
            hasCounter: "Returns whether the source document has a counter with this name. <strong>Context generation script only.</strong>",
            counter: "Returns a counter's value, or <code>null</code> when it does not exist. Available in both scripts.",
            counterRaw: "Returns the counter's per-node values rather than the aggregated total. Available in both scripts.",
        },
        timeSeries: {
            category: "Time series (read-only in GenAI scripts)",
            getTimeSeries: "Returns the names of the source document's time series, read from its metadata. Returns <code>false</code> when the document has no time series. Takes no arguments. <strong>Context generation script only.</strong>",
            hasTimeSeries: "Returns whether the source document has a time series with this name. <strong>Context generation script only.</strong>",
            getEntries: "Returns time series entries, optionally limited to a range. Available in both scripts.",
            getStats: "Returns <code>Start</code>, <code>End</code>, and <code>Count</code> statistics for the series. Available in both scripts.",
        },
        strings: {
            category: "String manipulation",
            startsWith: "Returns whether the string starts with the given prefix, ignoring case.",
            endsWith: "Returns whether the string ends with the given suffix, ignoring case.",
            regex: "Returns whether the string matches the regular expression. Evaluated server-side with a configurable timeout.",
            format: "Replaces <code>{0}</code>, <code>{1}</code>, ... placeholders in the string with the given arguments.",
        },
        arraysAndObjects: {
            category: "Arrays & objects",
            map: "Projects each value of an object or array into an array. <code>mapFunction</code> is called with <code>(value, key)</code>; the optional <code>context</code> becomes its <code>this</code>.",
        },
        math: {
            category: "Mathematical operations",
            min: "Returns the smaller of the two values, using RavenDB's comparison rules.",
            max: "Returns the larger of the two values, using RavenDB's comparison rules.",
        },
        spatial: {
            category: "Spatial",
            distance: "Returns the great-circle distance between two points. <code>units</code> may be <code>\"kilometers\"</code>, <code>\"miles\"</code> or <code>\"cartesian\"</code> — the last one returns a plain cartesian distance instead.",
        },
        conversion: {
            category: "Conversion & dates",
            scalarToRawString: "Reads a field exactly as stored, without RavenDB's automatic type conversion — useful for date strings you want to pass to the model verbatim.",
            convertJsTimeToTimeSpanString: "Converts a duration in milliseconds to a .NET <code>TimeSpan</code> string, e.g. <code>\"00:05:00\"</code>.",
            convertToTimeSpanString: "Builds a .NET <code>TimeSpan</code> string from ticks, or from the individual time components.",
            compareDates: "Compares or subtracts two dates. <code>operationType</code> may be <code>\"Subtract\"</code>, <code>\"GreaterThan\"</code>, <code>\"GreaterThanOrEqual\"</code>, <code>\"LessThan\"</code>, <code>\"LessThanOrEqual\"</code>, <code>\"Equal\"</code>, or <code>\"NotEqual\"</code>.",
            toStringWithFormat: "Formats a date, number, boolean, or date string using an optional .NET format string and culture, e.g. <code>\"yyyy-MM-dd\"</code>. The second argument may be either a format or a culture.",
        },
        crypto: {
            category: "Cryptographic methods",
            randomUuid: "Returns a random RFC 4122 version 4 UUID.",
            getRandomValues: "Fills the given typed array with cryptographically strong random values and returns it.",
            getRandomValuesBase64: "Returns the requested number of random bytes, base64-encoded.",
            digest: "Returns the base64 hash of <code>data</code>. <code>algorithm</code> may be <code>\"SHA-256\"</code>, <code>\"SHA-384\"</code> or <code>\"SHA-512\"</code>. The async <code>crypto.subtle.digest</code> is not available.",
            sign: "Returns a base64 HMAC signature of <code>data</code> using <code>key</code>.",
            verify: "Verifies a base64 HMAC signature produced by <code>crypto.sign</code>.",
            encryptAesGcm: "Encrypts <code>data</code> with AES-GCM and returns base64 ciphertext. The async <code>crypto.subtle.encrypt</code> is not available.",
            decryptAesGcm: "Decrypts AES-GCM ciphertext. <code>outputType</code> may be <code>\"string\"</code>, <code>\"raw\"</code>, or <code>\"buffer\"</code>.",
        },
        debugging: {
            category: "Debugging",
            output: "Writes a message to the test output. Use it together with <strong>Test context</strong> or the playground to inspect what the script is doing.",
        },
    },
    validation: {
        sampleObjectOrSchemaRequired: "Either 'Sample response object' or 'JSON schema' must be provided",
        schemaRegenerationRequired:
            "The sample object has been modified. Please regenerate the JSON schema to ensure it matches the new sample object structure",
        toolNameFormat: "Tool name can only contain letters, numbers, underscores and hyphens",
        toolNameUnique: "Tool name must be unique",
    },
} as const;
