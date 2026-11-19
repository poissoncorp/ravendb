import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;

const t = i18nModule.createTranslator("documentsCommands");

class saveRevisionsForConflictsConfigurationCommand extends commandBase {
    private readonly db: database | string;
    private readonly revisionsConfiguration: RevisionsCollectionConfiguration;

    constructor(db: database | string, revisionsConfiguration: RevisionsCollectionConfiguration) {
        super();
        this.db = db;
        this.revisionsConfiguration = revisionsConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.adminRevisions.adminRevisionsConflictsConfig;
        const args = ko.toJSON(this.revisionsConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("saveRevisionsForConflictsConfigurationFailed"), response.responseText, response.statusText));

    }
}

export = saveRevisionsForConflictsConfigurationCommand;
