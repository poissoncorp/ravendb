import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import DocumentsCompressionConfiguration = Raven.Client.ServerWide.DocumentsCompressionConfiguration;

const t = i18nModule.createTranslator("documentsCommands");

class saveDocumentsCompressionCommand extends commandBase {
    private readonly db: database | string;
    private readonly config: DocumentsCompressionConfiguration;

    constructor(db: database | string, config: DocumentsCompressionConfiguration) {
        super();
        this.db = db;
        this.config = config;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.documentsCompression.adminDocumentsCompressionConfig;
        
        return this.post<updateDatabaseConfigurationsResult>(url, JSON.stringify(this.config), this.db)
            .done(() => this.reportSuccess(t("documentsCompressionSaved")))
            .fail((response: JQueryXHR) => this.reportError(t("saveDocumentsCompressionFailed"), response.responseText, response.statusText));
    }
}

export = saveDocumentsCompressionCommand;
