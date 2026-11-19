import dialog = require("plugins/dialog");
import deleteDocumentsCommand = require("commands/database/documents/deleteDocumentsCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("deleteDocuments");

class deleteDocuments extends dialogViewModelBase {

    view = require("views/common/deleteDocuments.html");

    private documentIds = ko.observableArray<string>();
    deletingDocumentHtml = ko.pureComputed(() =>
        t("deletingDocument", { documentId: generalUtils.escapeHtml(this.documentIds()[0]) })
    );
    deletingDocumentsHtml = ko.pureComputed(() => t("deletingDocuments", { count: this.documentIds().length }));
    private deletionStarted = false;
    deletionTask = $.Deferred<void>();

    constructor(documentIds: Array<string>, private db: database | string, private transactionMode: Raven.Client.Documents.Session.TransactionMode = "SingleNode") {
        super(null);

        if (documentIds.length === 0) {
            throw new Error("Must have at least one document id to delete.");
        }

        this.documentIds(documentIds);
    }

    deleteDocs() {
        const docCount = this.documentIds().length;
        const documentId = this.documentIds()[0];

        new deleteDocumentsCommand(this.documentIds(), this.db, this.transactionMode)
            .execute()
            .done(() => {
                messagePublisher.reportSuccess(
                    docCount === 1
                        ? t("deletedDocument", { documentId })
                        : t("deletedDocuments", { count: docCount })
                );
                this.deletionTask.resolve();
            })
            .fail(response => {
                messagePublisher.reportError(
                    docCount === 1
                        ? t("deleteDocumentFailed", { documentId })
                        : t("deleteDocumentsFailed", { count: docCount }),
                    response.responseText,
                    response.statusText);
                this.deletionTask.reject(response);
            });

        this.deletionStarted = true;
        dialog.close(this, true);
    }

    cancel() {
        dialog.close(this, false);
    }

    deactivate(args: any) {
        super.deactivate(args);
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.deletionStarted) {
            this.deletionTask.reject();
        }
    }
}

export = deleteDocuments;
