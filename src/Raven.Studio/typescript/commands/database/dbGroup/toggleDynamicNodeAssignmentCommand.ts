import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("dbGroupCommands");

class toggleDynamicNodeAssignmentCommand extends commandBase {

    private databaseName: string;

    private enable: boolean;

    constructor(databaseName: string, enable: boolean) {
        super();
        this.enable = enable;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.databaseName,
            enable: this.enable
        };
        const url = endpoints.global.adminDatabases.adminDatabasesDynamicNodeDistribution + this.urlEncodeArgs(args);

        return this.post<void>(url, null, null,  { dataType: undefined })
            .done(() => this.reportSuccess(t("dynamicDistributionToggled", { context: this.enable ? "on" : "off" })))
            .fail((response: JQueryXHR) => this.reportError(t("dynamicDistributionToggleFailed"), response.responseText, response.statusText));
    }
}

export = toggleDynamicNodeAssignmentCommand;
