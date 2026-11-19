import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class testRavenEtlCommand extends commandBase {
    constructor(private db: database | string, private payload: TestRavenEtlScript) {
        super();
    }  

    execute(): JQueryPromise<RavenEtlTestScriptResult> {
        const url = endpoints.databases.ravenEtl.adminEtlRavenTest;

        return this.post<RavenEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(t("ravenEtlTestFailed"), response.responseText, response.statusText);
            });
    }
}

export = testRavenEtlCommand; 

