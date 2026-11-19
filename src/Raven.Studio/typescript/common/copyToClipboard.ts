import messagePublisher = require("common/messagePublisher");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("studioCommon");

class copyToClipboard {
    static copy(toCopy: string, successMessage?: string, container: Element = document.body) {
        const dummy = document.createElement("textarea");
        // Add it to the document
        container.appendChild(dummy);
        try {
            dummy.value = toCopy;
            // Select it
            dummy.select();
            // Copy its contents
            const success = document.execCommand("copy");

            if (success) {
                if (successMessage) {
                    messagePublisher.reportSuccess(successMessage);
                }
            } else {
                messagePublisher.reportWarning(t("copyToClipboardFailed"));
            }
        } catch (err) {
            messagePublisher.reportWarning(t("copyToClipboardFailed"), err);
        } finally {
            // Remove it as its not needed anymore
            container.removeChild(dummy);
        }
    }
}

export = copyToClipboard;
