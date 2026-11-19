import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;

const t = i18nModule.createTranslator("documentsCommands");

class saveRevisionsConfigurationCommand extends commandBase {
    private readonly db: database | string;
    private readonly revisionsConfiguration: RevisionsConfiguration;

    constructor(db: database | string, revisionsConfiguration: RevisionsConfiguration) {
        super();
        this.db = db;
        this.revisionsConfiguration = revisionsConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.adminRevisions.adminRevisionsConfig;
        const args = ko.toJSON(this.revisionsConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("saveRevisionsConfigurationFailed"), response.responseText, response.statusText));

    }
}

export = saveRevisionsConfigurationCommand;
