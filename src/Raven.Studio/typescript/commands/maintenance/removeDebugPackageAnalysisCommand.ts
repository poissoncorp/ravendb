import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class removeDebugPackageAnalysisCommand extends commandBase {

    constructor(private packageId: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerRemove +
            this.urlEncodeArgs({ packageId: this.packageId });

        return this.del<void>(url, null, null, { dataType: "text" })
            .fail((response: JQueryXHR) =>
                this.reportError(t("removeDebugPackageAnalysisFailed"), response.responseText, response.statusText));
    }
}

export = removeDebugPackageAnalysisCommand;
