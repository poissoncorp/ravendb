import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import CdcTestRequest = Raven.Client.Documents.Operations.CdcSink.Test.CdcTestRequest;
import CdcTestResult = Raven.Client.Documents.Operations.CdcSink.Test.CdcTestResult;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

export default class verifyCdcSinkCommand extends commandBase {
    constructor(
        private db: string,
        private payload: CdcTestRequest
    ) {
        super();
    }

    execute(): JQueryPromise<CdcTestResult> {
        const url = endpoints.databases.cdcSink.adminCdcSinkDryRun;

        return this.post<CdcTestResult>(url, JSON.stringify(this.payload), this.db).fail(
            (response: JQueryXHR) => {
                this.reportError(t("cdcSinkVerifyFailed"), response.responseText, response.statusText);
            }
        );
    }
}
