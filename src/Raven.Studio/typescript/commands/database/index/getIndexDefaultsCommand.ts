import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexDefaultsCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.Processors.IndexDefaults> {
        const url = endpoints.databases.studioDatabaseTasks.studioTasksIndexesConfigurationDefaults;
        return this.query<Raven.Server.Web.Studio.Processors.IndexDefaults>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("getDefaultsFailed"), response.responseText, response.statusText);
            });
    }
}

export = getIndexDefaultsCommand;
