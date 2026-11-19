import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class getSetupLocalNodeIpsCommand extends commandBase {

    execute(): JQueryPromise<string[]> {      
        const url = endpoints.global.setup.setupIps;
        
        return this.query<string[]>(url, null, null, x => x.NetworkInterfaces.flatMap((i: any) => i.Addresses))
            .fail((response: JQueryXHR) => this.reportError(t("getSetupLocalNodeIpsFailed"), response.responseText, response.statusText));            
    }
}

export = getSetupLocalNodeIpsCommand;
