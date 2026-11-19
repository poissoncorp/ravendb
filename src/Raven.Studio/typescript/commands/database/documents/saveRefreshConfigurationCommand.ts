import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;

const t = i18nModule.createTranslator("documentsCommands");

class saveRefreshConfigurationCommand extends commandBase {
    
    private readonly db: database | string;
    private readonly refreshConfiguration: RefreshConfiguration;
    
    constructor(db: database | string, refreshConfiguration: RefreshConfiguration) {
        super();
        this.db = db;
        this.refreshConfiguration = refreshConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.refresh.adminRefreshConfig;
        const args = ko.toJSON(this.refreshConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("saveRefreshConfigurationFailed"), response.responseText, response.statusText));
    }
}

export = saveRefreshConfigurationCommand;
