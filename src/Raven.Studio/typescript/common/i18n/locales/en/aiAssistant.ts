export default {
    button: "AI Assistant",
    disabledInSettings: "AI Assistant is disabled (Ai.Assistant.Disable).<br/>It can be edited from the settings.json file.",
    window: {
        title: "AI Assistant",
        refining: "Text refine in progress... Please wait.",
        failed: "Failed to assist. Please try again.",
        discard: "Discard",
        accept: "Accept",
    },
    consent: {
        checking: "Checking consent... Please wait.",
        checkFailed: "Failed to check consent. <button>Please try again</button>",
        required:
            "To use our built-in AI features, such as <em>AI Assistant</em>, you need to provide consent. The feature will remain unavailable until accepted.",
        review: "Review the consent",
    },
    eula: {
        title: "Get Started with AI Assistant",
        description:
            "The built-in AI Assistant is designed to supercharge your workflow. To enable this feature, please review and accept the Terms of Use.",
        accept: "I accept the <a>RavenDB AI Assistant Terms of Use</a>",
        agree: "Agree & Enable",
    },
    errors: {
        failedToCheckConsent: "Failed to check consent. Please try again.",
        invalidCredentials: "Invalid license. Please use a different license.",
        invalidData: "Invalid data. Please check your data and try again.",
        outOfTokens:
            "You have used all your AI Assistant tokens for this month. Your token allowance will be reset at the beginning of the next month.",
    },
} as const;
