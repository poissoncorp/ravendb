import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class getClusterObserverDecisionsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions> {
        
        const url = endpoints.global.rachisAdmin.adminClusterObserverDecisions;

        return this.query<Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getObserverDecisionsFailed"), response.responseText));
    }
}

export = getClusterObserverDecisionsCommand;
