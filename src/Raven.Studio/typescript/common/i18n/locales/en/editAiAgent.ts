export default {
    title_edit: "Edit AI Agent",
    title_create: "Create AI Agent",
    loadError: "Unable to load configuration",
    about: {
        intro: "In this view, you can define an AI Agent - <br/> a natural language <strong>conversational assistant</strong> powered by an LLM.",
        chat: "The agent lets you chat with an LLM about your data. It can retrieve information from your database to answer prompts, and trigger specific actions when needed.",
        configurationIncludes: "The configuration includes:",
        connectionString:
            "<strong>Connection string</strong> - Specifies the AI provider and the LLM of your choice.",
        responseStructure: "<strong>Response structure</strong> - Define the expected format of the agent’s replies.",
        chatStorage:
            "<strong>Chat storage</strong> - Configure the documents that store chat conversations.<br/> You can optionally enable trimming or summarization of their content.",
        tools: "<strong>Tools</strong> - Tools are a controlled way to pass context to the LLM:",
        queryTools:
            "<strong>Query tools</strong>: <br/> Define queries the agent can run against your database when the LLM thinks they’re needed to respond to the user. Scope access by setting agent parameters - ensuring the LLM receives only the data its allowed to access.",
        actionTools:
            "<strong>Action tools</strong>: <br/> Define tasks the agent can trigger when instructed by the LLM in response to user prompts.",
        subAgents:
            "<strong>Sub-agents</strong> - Assign other AI agents that this agent can delegate specialized tasks to. The sub-agent runs its own tools (queries, actions) and returns its response to the parent agent.",
        overviewDocsLink: "Docs - AI Agents Overview",
        createDocsLink: "Docs - Create AI Agent",
    },
    shared: {
        description: "Description",
        syntaxExample: "Syntax example",
        exampleNumbered: "Example {{number}}:",
        exampleNoParameters: "Example {{number}} (no parameters will be used):",
    },
    basic: {
        heading: "Configure basic settings",
        description: "Define your agent's purpose, its AI provider connection, and the structure of its responses.",
        nameLabel: "Agent name",
        namePlaceholder: "e.g. Customer Service Agent",
        identifierLabel: "Identifier",
        identifierInfo:
            "A unique identifier for the agent.<br/>If not specified, it will be auto-generated from the agent name.",
        identifierPlaceholder: "e.g. customer-service-agent",
        regenerateTitle: "Click to generate the identifier from the agent name.",
        regenerate: "Regenerate",
        stateLabel: "Agent State",
        stateEnabled: "Enabled",
        stateDisabled: "Disabled",
        systemPromptLabel: "System prompt",
        systemPromptInfo:
            "This prompt defines the agent's role and capabilities.<br/>It provides general context to guide the LLM's responses throughout the conversation.",
        systemPromptPlaceholder:
            "Describe the agent's purpose and capabilities. \nE.g.: You are a customer support assistant for an e-commerce platform, capable of answering questions about products and orders.\nYou can also assist with returns, refunds, and order issues by triggering the appropriate action to escalate to the support team.",
        aiAssistantSuccess: "AI refined your prompt based on your input.",
        sampleObjectLabel: "Sample response object",
        sampleObjectInfo:
            "Provide a JSON object that defines the structure of the responses you expect to receive from the LLM via the agent in the conversation.<br/><br/>This object is not sent to the model directly - RavenDB uses it to generate a JSON schema, which is sent to the model.",
        sampleObjectPlaceholder:
            '{\n    // "ResponseField: "Instruction to the LLM",\n    // ...\n    // "ResponseField" is a custom field name that you want the LLM to include in its response.\n    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what content to return in this field.\n} \nOpen the (?) icon to view an example.',
        sampleObjectExample: "Example of a sample object that defines the expected response structure:",
        jsonSchemaLabel: "Response JSON schema",
        jsonSchemaInfo:
            "This JSON schema defines the structure of the response you expect the LLM to reply with.<br/>It is included in the request sent to the model.<br/><br/>If you don't provide a schema, RavenDB will generate one automatically based on the sample response object.<br/><br/>If you provide both a sample object and a schema, the schema takes precedence.",
        jsonSchemaExample: "Example of a JSON schema that defines the expected response structure:",
    },
    parameters: {
        heading: "Set agent parameters",
        headingInfo:
            "Define query parameters that the agent will replace with fixed values before executing a query tool against the database. This ensures that queries run only within the allowed data scope.<br/><br/>You will need to provide values for these parameters when starting a new chat with the agent.",
        description:
            "Define query parameters that the agent will replace with fixed values before executing a query tool against the database.",
        listTitle: "Agent parameters",
        add: "Add new parameter",
        empty: "No parameters have been defined yet",
        configure: "Configure parameter",
        nameLabel: "Parameter name",
        namePlaceholder: "e.g. company",
        typeLabel: "Parameter type",
        typePlaceholder: "Select a type",
        descriptionPlaceholder: "e.g. The company ID",
        policyLabel: "Forbid model generation",
        policyInfo:
            "When <b>Forbid model generation</b> is set and this agent is used as a sub-agent, the parent agent is not allowed to generate a parameter value for the sub-agent.<br/>The parameter's value may only be inherited from the parent agent parameters. This ensures that this is a trusted value.",
        policyPlaceholder: "Select a value",
        sendToModel: "Send to model",
        sendToModelInfo:
            "When enabled, the parameter is exposed to the model. It is included in the prompt sent to the model and in any echo messages.<br/><br/>When disabled, the parameter is hidden from the model. It is excluded from both the prompt and echo messages.",
        types: {
            string: "String",
            number: "Number",
            boolean: "Boolean",
            arrayOfString: "String[]",
            arrayOfNumber: "Number[]",
            arrayOfBoolean: "Boolean[]",
            any: "Any",
            null: "Null",
        },
        policies: {
            default: "Default",
            forbidModelGeneration: "Forbid model generation",
        },
    },
    tools: {
        heading: "Define agent tools",
        headingInfo:
            "Tools are a controlled way to pass context to the LLM. Configure the tools that the LLM can instruct the agent to trigger in response to user prompts.<br/><br/>These include query tools (to retrieve data from the database) and action tools (to initiate tasks that are expected to be carried out by the client or user).",
        description: "Tools are a controlled way to pass context to the LLM.",
        queryToolsTitle: "Query tools",
        queryToolsInfo:
            'Define queries the agent is allowed to execute against the database in order to retrieve data.<br/><br/>The LLM can instruct the agent to run these queries as needed to answer user questions.<br/><br/>You can restrict the query scope by filtering results using the defined "agent parameters".',
        addQueryTool: "Add new query tool",
        actionToolsTitle: "Action tools",
        actionToolsInfo:
            "Define actions that the agent can trigger when requested by the LLM, allowing the backend or client to perform operations in response to user prompts and conversation context.<br/><br/>Each action tool should handle a specific task in your system - for example, creating a support ticket, sending a notification, or updating a document",
        addActionTool: "Add new action tool",
        toolNameLabel: "Tool name",
        sampleParametersLabel: "Sample parameters object",
        parametersSchemaLabel: "Parameters JSON schema",
    },
    queryTool: {
        configure: "Configure query tool",
        namePlaceholder: "e.g. GetOrdersByCountryAndCompany",
        descriptionPlaceholder:
            "In this description, explain to the LLM when it should trigger this query.\nExample 1: Use this query to retrieve Order documents from the database filtered by destination country and company.\nExample 2: Use this query to perform a semantic search for products similar to those in the customer's order.",
        queryLabel: "Query",
        testQueryTitle: "Click to test this query in the Studio's Query View",
        testQuery: "Test query",
        queryPlaceholder:
            '// Enter the query that will run against the database. \n// You can query an existing static index or make a dynamic query. For example:\n// Example 1: from "Orders" where ShipTo.Country == $country" and Company == $company"\n// Example 2: from "Products" where vector.search(embedding.text(Name), $searchTerm, $similarityLevel)\n\n// Parameters (i.e. $paramName) that are defined in the "Set agent parameters" section will be replaced with the fixed values you provide.\n// Other parameters (i.e. $paramName) will be filled with the values provided by the LLM when it calls this query tool.',
        sampleObjectInfo:
            "Provide a JSON object that defines the parameters the LLM is expected to supply when it requests the agent to execute this query tool.<br/><br/>This object is not sent to the model directly - RavenDB uses it to generate a JSON schema, which is sent to the model.",
        sampleObjectPlaceholder:
            '{\n    // "ParamName": "Instruction to the LLM",\n    // ... \n    // "ParamName" is the name of a parameter from the query for which the LLM needs to provide a value.\n    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what value to supply in this field.\n}\nOpen the (?) icon to view an example.',
        jsonSchemaExample: "Example:",
        advancedSettings: "Advanced settings",
        allowModelQueries: "Allow model queries",
        allowModelQueriesInfo:
            "When True, the model is allowed to execute this query on demand based on its own judgment.<br/><br/>When False, the model cannot call this query (unless executed as part of initial context).<br/><br/>When Default, server default is applied.",
        selectTrueOrFalse: "Select True or False",
        defaultPlaceholder: "Default",
        override: "Override",
        addToInitialContext: "Add to initial context",
        addToInitialContextInfo:
            "When True, the query will be executed during the initial context build and its results provided to the model.<br/><br/>When False, the query will not be executed for the initial context.<br/><br/>When Default, server default is applied.",
        valueTrue: "True",
        valueFalse: "False",
    },
    actionTool: {
        configure: "Configure action tool",
        namePlaceholder: "e.g. ContactSupportTeam",
        descriptionPlaceholder:
            "In this description, explain to the LLM under which conditions it should trigger this action.\nE.g., Trigger this action tool when you identify the customer has a problem with an order.",
        sampleObjectInfo:
            "This JSON object defines the format in which the LLM will supply data for the requested action when it decides to trigger this action tool.<br/>The LLM will fill in values for the specified fields based on the conversation context and any relevant data it has access to.<br/><br/>You can then pass this data to your client or backend to handle the requested task. After the task is completed, you should reply to the LLM with a free-text message describing the result of the action.<br/><br/>This object is not sent to the model directly - RavenDB uses it to generate a JSON schema, which is sent to the model.<br/>This object is optional. Providing an empty object <code>{}</code> is also valid if you don't need any data from the LLM.",
        sampleObjectPlaceholder:
            '{\n    // "FieldName": "Instruction to the LLM",\n    // ...\n    // "FieldName" is a custom field that you want the LLM to provide when triggering the action.\n    // The value ("Instruction to the LLM") is a natural-language instruction that tells the LLM what content to provide in this field.\n}\nOpen the (?) icon to view an example.',
        sampleObjectExample:
            "This example shows a sample object you can define to specify the data format the LLM should provide when it decides to trigger this action tool.<br/><br/>The values the LLM will supply can be based on the ongoing conversation or on data retrieved using query tools.",
    },
    subAgents: {
        heading: "Define sub-agents",
        headingInfo:
            "Sub-agents are other AI agents that this parent agent can trigger when requested by the LLM to handle specialized tasks.<br/><br/>When the LLM determines it should delegate work to a sub-agent, it generates a natural-language prompt for that sub-agent. The parent agent then invokes the sub-agent and forwards the prompt. The sub-agent runs its own tools (queries, actions) to complete the task and returns its response to the parent agent.",
        description: "Choose existing agents to act as sub-agents for this one.",
        listTitle: "Sub-agents",
        listInfo:
            "Add the agents that this agent is allowed to delegate work to. The description you provide tells the LLM when it should trigger that specific sub-agent.",
        add: "Add new sub-agent",
        configure: "Configure sub-agent",
        identifierLabel: "Agent identifier",
        notFound: "Warning: This agent is currently not found in your database",
        descriptionPlaceholder:
            "In this description, explain to the LLM when it should delegate to this sub-agent. E.g., Use this agent to handle all questions about orders, order history, and order status.",
    },
    trimming: {
        heading: "Configure chat trimming",
        headingInfo:
            'You can configure trimming of long conversations by summarizing older messages.<br/><br/>If you "enable history", the original chat content prior to trimming will be stored in dedicated documents in the <code>@conversations-history</code> collection.<br/><br/>Note: if there are any open action tools that have not yet received a response, trimming will be delayed until those actions are completed.',
        description: "You can configure trimming of long conversations by summarizing older messages.",
        methodLabel: "Trimming method",
        clearSelection: "Clear selection",
        summarizeTitle: "Summarize chat",
        summarizeDescription:
            "Summarize the chat conversation into a compact prompt when the total number of tokens used exceeds the configured threshold.",
        maxTokensBefore: "Max tokens before summarization",
        maxTokensBeforeInfo:
            "Summarization will be triggered when the total number of tokens used in the conversation exceeds this limit.",
        maxTokensAfter: "Max tokens after summarization",
        maxTokensAfterInfo:
            "The maximum number of tokens to retain in the conversation after summarization.<br/>Messages exceeding this limit will be removed, starting from the oldest.",
        defaultPlaceholder: "Default ({{value}})",
        enableHistory: "Enable history",
        enableHistoryInfo:
            "Toggle on to keep the original chat content before trimming in a dedicated document in the <code>@conversations-history</code> collection.",
        setHistoryExpiration: "Set history expiration",
        setHistoryExpirationInfo: "Toggle on to set how long history documents are retained.",
    },
    footer: {
        cancelTitle: "Cancel configuration and return to the ai agents list.",
        testInfo: "Click to test the agent in the test area.",
        test: "Test",
    },
    test: {
        heading: "Test agent",
        parametersNotUpToDate: "The parameters are not up to date. Please start a new chat.",
        newChatTitle: "Clear the current conversation and start a new chat",
        newChat: "New chat",
        rawDataTitle: "Switch between chat and raw data display",
        promptPlaceholder: "Ask the agent anything",
        subConversationNotAvailable: "Sub-conversation ({{id}}) data is not available",
        agentIdNotAvailable: "Agent ID is not available for sub-conversation ({{id}})",
        agentConfigurationNotAvailable: "Agent configuration is not available for sub-conversation ({{id}})",
        transcriptTitle: "Sub-agent test transcript",
        identifier: "Identifier: <b>{{identifier}}</b>",
        agentIdCopied: "Agent ID copied to clipboard",
        notStored: "This conversation exists only in the current test result and is not stored in the database.",
        close: "Close",
    },
    validation: {
        sampleObjectOrSchemaRequired: "Either 'Sample response object' or 'JSON schema' must be provided",
        schemaRegenerationRequired:
            "The sample object has been modified. Please regenerate the JSON schema to ensure it matches the new sample object structure",
        parameterNameUnique: "Parameter name must be unique",
        toolNameFormat: "Tool name can only contain letters, numbers, underscores and hyphens",
        toolNameUnique: "Tool name must be unique",
    },
} as const;
