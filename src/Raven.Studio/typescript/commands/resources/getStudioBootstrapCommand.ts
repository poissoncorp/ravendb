import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class getStudioBootstrapCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.StudioBootstrapConfiguration> {
        const url = endpoints.global.studioTasks.studioTasksBootstrap;
        
        return this.query<Raven.Server.Web.Studio.StudioTasksHandler.StudioBootstrapConfiguration>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError(t("getStudioBootstrapFailed"), response.responseText, response.statusText);
            });
    }
}

export = getStudioBootstrapCommand;
