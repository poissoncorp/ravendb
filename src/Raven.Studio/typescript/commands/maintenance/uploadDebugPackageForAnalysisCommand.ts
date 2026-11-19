import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

class uploadDebugPackageForAnalysisCommand extends commandBase {

    constructor(private file: File) {
        super();
    }

    execute(): JQueryPromise<DebugPackageAnalysisSummary> {
        const url = endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerUpload;

        // the server reads the raw .zip from the request body (not multipart), so send the File as-is
        const options: JQueryAjaxSettings = {
            processData: false,
            contentType: false,
            cache: false,
        };

        return this.post<DebugPackageAnalysisSummary>(url, this.file, null, options, 60000)
            .fail((response: JQueryXHR) =>
                this.reportError(t("analyzeDebugPackageFailed"), response.responseText, response.statusText));
    }
}

export = uploadDebugPackageForAnalysisCommand;
