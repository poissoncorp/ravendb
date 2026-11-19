import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import Schema = Raven.Client.Documents.Operations.CdcSink.Schema;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

export default class getCdcSinkTaskSchemaCommand extends commandBase {
    constructor(
        private db: string,
        private payload: Schema.CdcSinkSchemaRequest
    ) {
        super();
    }

    execute(): JQueryPromise<Schema.CdcSinkSourceSchema> {
        const url = endpoints.databases.cdcSink.adminCdcSinkSchema;

        return this.post<Schema.CdcSinkSourceSchema>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError(t("cdcSinkSchemaFailed"), response.responseText, response.statusText);
            }
        );
    }
}
