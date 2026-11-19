import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class adminJsScriptCommand extends commandBase {
    
    private readonly script: string;
    private readonly targetDatabase: string;
    
    constructor(script: string, targetDatabase?: string) {
        super();
        this.script = script;
        this.targetDatabase = targetDatabase;
    }

    execute(): JQueryPromise<{Result: any}> {
        const args = {
            serverScript: !this.targetDatabase,
            database: this.targetDatabase
        };

        const payload: Raven.Server.Documents.Patch.AdminJsScript = {
            Script: this.script
        };

        const url = endpoints.global.adminDatabases.adminConsole + this.urlEncodeArgs(args);
        return this.post(url, JSON.stringify(payload))
            .done(() => this.reportSuccess(t("adminJsScriptSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("adminJsScriptFailed"), response.responseText, response.statusText));
    }
}

export = adminJsScriptCommand;
