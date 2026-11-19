import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class deleteAttachmentCommand extends commandBase {

    constructor(private documentId: string, private attachmentName: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.documentId,
            name: this.attachmentName
        };
        const url = endpoints.databases.attachment.attachments + this.urlEncodeArgs(args);
        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(t("attachments.deleted")))
            .fail((response: JQueryXHR) => this.reportError(t("attachments.deleteFailed", { attachmentName: this.attachmentName }), response.responseText, response.statusText));
    }
}

export = deleteAttachmentCommand;
