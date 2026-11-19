export default {
    about: {
        intro: "In this view, you can manage AI tasks - <br/>create new tasks, edit existing ones, or delete them as needed.",
        genAi: {
            heading: "<strong>GenAI tasks</strong>:",
            enrichDocuments: "Use large language models (LLMs) to analyze and enrich your documents.",
            updateDocuments: "Automatically update documents or create new ones based on AI-generated content.",
        },
        embeddings: {
            heading: "<strong>Embeddings generation tasks</strong>:",
            extractText: "Extract text from your documents,",
            generateEmbeddings: "Connect to an AI service to generate embeddings from that text,",
            saveEmbeddings: "Save the generated embeddings in dedicated collections in the database.",
            vectorSearch: "Use the embeddings to perform vector search queries.",
        },
        genAiDocsLink: "Docs - GenAI Task Overview",
        embeddingsDocsLink: "Docs - Generating Embeddings Overview",
    },
} as const;
