import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class toggleDatabaseCommand extends commandBase {

    private readonly databaseNames: string[];

    private readonly enable: boolean;

    constructor(databaseNames: string[], enable: boolean) {
        super();
        this.enable = enable;
        this.databaseNames = databaseNames;
    }

    get action() {
        return this.enable ? "enable" : "disable";
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const payload: Raven.Client.ServerWide.Operations.ToggleDatabasesStateOperation.Parameters = {
            DatabaseNames: this.databaseNames
        };

        const url = this.enable ?
            endpoints.global.adminDatabases.adminDatabasesEnable :
            endpoints.global.adminDatabases.adminDatabasesDisable;

        return this.post(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError(t("toggleFailed"), response.responseText, response.statusText));
    }

}

export = toggleDatabaseCommand;  
