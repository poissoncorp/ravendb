import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveDatabaseSettingsCommand extends commandBase {
   
    constructor(private db: database | string, private settingsToSave: Record<string, string>) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        
        const url = endpoints.global.adminConfiguration.adminConfigurationSettings;
        
        return this.put<void>(url, JSON.stringify(this.settingsToSave), this.db, { dataType: undefined })
            .done(() => this.reportSuccess(t("saveDatabaseSettingsSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("saveDatabaseSettingsFailed"), response.responseText, response.statusText));
    }
}

export = saveDatabaseSettingsCommand;
