import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");
import ServerStatistics = Raven.Server.ServerWide.ServerStatistics;

class getAdminStatsCommand extends commandBase {

    private readonly nodeTag: string;
    
    constructor(nodeTag: string) {
        super();
        this.nodeTag = nodeTag;
    }
    
    execute(): JQueryPromise<ServerStatistics> {
        const url = endpoints.global.adminStats.adminStats;
        const args = {
            nodeTag: this.nodeTag,
        }

        return this.query<ServerStatistics>(url, args, undefined)
            .fail((response: JQueryXHR) => this.reportError(t("getAdminStatsFailed"), response.responseText, response.statusText));
    }
}

export = getAdminStatsCommand;
