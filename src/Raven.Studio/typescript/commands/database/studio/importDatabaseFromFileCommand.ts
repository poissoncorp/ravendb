/// <reference path="../../../../typings/tsd.d.ts" />

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("studioCommands");

class importDatabaseFromFileCommand extends commandBase {

    constructor(private db: database | string, private operationId: number, private file: File,
        private importOptions: Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions,
        private onUploadProgress: (percentComplete: number) => void) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId
        };

        const url = endpoints.databases.smuggler.smugglerImport + this.urlEncodeArgs(urlArgs);

        const formData = new FormData();

        formData.append("importOptions", JSON.stringify(this.importOptions, (key, value) => {
            if (key === "TransformScript" && value === "") {
                return undefined;
            }
            return value;
        }));

        formData.append("file", this.file);

        // must start true: getOptionsForImport's progress handler early-returns when !isUploading()
        const isUploading = ko.observable<boolean>(true);
        const uploadStatus = ko.observable<number>(0);
        uploadStatus.subscribe((percent) => this.onUploadProgress(percent));

        return this.post(url, formData, this.db, commandBase.getOptionsForImport(isUploading, uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError(t("uploadFailed"), response.responseText, response.statusText));
    }
}

export = importDatabaseFromFileCommand;
