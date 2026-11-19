import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

interface UploadAttachmentCommandArgs {
    id: string;
    name: string;
    contentType: string;
    remoteAt?: string;
    remoteIdentifier?: string;
}
class uploadAttachmentCommand extends commandBase {

    private xhr: XMLHttpRequest;
    
    constructor(private file: File, private documentId: string, private db: database | string, private onProgress?: (event: ProgressEvent) => void, private remoteParameters?: Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters) {
        super();
    }
    
    abort() {
        if (this.xhr) {
            this.xhr.abort();    
        }
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Attachments.AttachmentDetails> {
        const args: UploadAttachmentCommandArgs = {
            id: this.documentId,
            name: this.file.name,
            contentType: this.file.type,
        };

        if (this.remoteParameters) {
            args.remoteAt = this.remoteParameters.At;
            args.remoteIdentifier = this.remoteParameters.Identifier;
        }

        const options: JQueryAjaxSettings = {
            processData: false,
            cache: false,
            dataType: '',
            xhr: () => {
                const xhr = new XMLHttpRequest();
                xhr.upload.addEventListener("progress", (evt: ProgressEvent) => {
                    if (this.onProgress) {
                        this.onProgress(evt);
                    }
                }, false);
                
                this.xhr = xhr;
                return xhr;
            }
        };

        const url = endpoints.databases.attachment.attachments + this.urlEncodeArgs(args);
        return this.put<Raven.Client.Documents.Operations.Attachments.AttachmentDetails>(url, this.file, this.db, options, 0)
            .done(() => {
                this.reportSuccess(t("attachments.uploaded", { fileName: this.file.name }));
            })
            .fail((response: JQueryXHR) => this.reportError(t("attachments.uploadFailed"), response.responseText, response.statusText));
    }
}

export = uploadAttachmentCommand;
