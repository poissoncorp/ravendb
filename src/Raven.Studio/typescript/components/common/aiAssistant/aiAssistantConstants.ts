import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("aiAssistant");

export const aiAssistantConstants = {
    get failedToCheckConsent() {
        return t("errors.failedToCheckConsent");
    },
    get invalidCredentials() {
        return t("errors.invalidCredentials");
    },
    get invalidData() {
        return t("errors.invalidData");
    },
    get outOfTokens() {
        return t("errors.outOfTokens");
    },
};
