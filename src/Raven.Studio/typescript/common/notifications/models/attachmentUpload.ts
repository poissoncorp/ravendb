/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("notifications");

class attachmentUpload extends abstractNotification {

    fileName: string;
    documentId: string;
    abort: () => void;

    loaded = ko.observable<number>(0);
    total = ko.observable<number>(1);

    isPercentageProgress = ko.observable<boolean>(false);
    percentageProgress = ko.pureComputed(() => this.loaded() * 100 / this.total());
    textualProgress = ko.pureComputed(() => generalUtils.formatBytesToSize(this.loaded()) + "/" + generalUtils.formatBytesToSize(this.total()));
    
    isCompleted = ko.pureComputed(() => this.loaded() === this.total());
    isCanceled  = ko.observable<boolean>(false);
    killable = ko.pureComputed(() => !this.isCompleted());
    
    static currentUploadId = 1;

    constructor(databaseName: string, dto: Raven.Server.NotificationCenter.Notifications.Notification, documentId: string, fileName: string) {
        super(databaseName, dto);
        
        this.requiresRemoteDismiss(false);

        this.documentId = documentId;
        this.fileName = fileName;
        
        this.updateWith(dto);
        this.createdAt(moment.utc());
    }
    
    abortUpload() {
        if (!this.isCompleted()) {
            this.abort();
        }
    }

    updateProgress(event: ProgressEvent): void {
        if (event.lengthComputable) {
            this.isPercentageProgress(true);
            
            this.loaded(event.loaded);
            this.total(event.total);
            
            if (this.isCompleted()) {
                this.message(
                    t("attachmentUpload.uploaded", {
                        fileName: generalUtils.escapeHtml(this.fileName),
                        size: generalUtils.formatBytesToSize(event.total),
                        documentId: generalUtils.escapeHtml(this.documentId),
                    })
                );
                this.severity("Success");
            }
        }
    }
    
    static forFile(databaseName: string, documentId: string, fileName: string) {
        return new attachmentUpload(databaseName, {
            Type: "AttachmentUpload",
            CreatedAt: null, // will be assigned later
            Database: databaseName,
            Message: t("attachmentUpload.uploading", {
                fileName: generalUtils.escapeHtml(fileName),
                documentId: generalUtils.escapeHtml(documentId),
            }),
            Title: t("attachmentUpload.title"),
            Severity: "None",
            IsPersistent: false,
            Id: "AttachmentUpload/" + (attachmentUpload.currentUploadId++)
        }, documentId, fileName);
    }

}

export = attachmentUpload;
