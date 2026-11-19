import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class getSetupParametersCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.SetupParameters> {      
        const url = endpoints.global.setup.setupParameters;
        
        return this.query<Raven.Server.Commercial.SetupParameters>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getSetupParametersFailed"), response.responseText, response.statusText));            
    }
}

export = getSetupParametersCommand;
