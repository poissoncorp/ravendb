import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getDocumentsCompressionConfigurationCommand extends commandBase {
    private readonly db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.DocumentsCompressionConfiguration> {
        const url = endpoints.databases.documentsCompression.documentsCompressionConfig;
        const getConfigurationTask = $.Deferred<Raven.Client.ServerWide.DocumentsCompressionConfiguration>();

        this.query<Raven.Client.ServerWide.DocumentsCompressionConfiguration>(url, null, this.db)
            .done(dto => getConfigurationTask.resolve(dto)) 
            .fail((response: JQueryXHR) => {
                if (response.status !== 404) {
                    this.reportError(t("getDocumentsCompressionConfigurationFailed"), response.responseText, response.statusText);
                    getConfigurationTask.reject(response);
                } else {
                    getConfigurationTask.resolve(null);
                }
            });

        return getConfigurationTask;
    }
}

export = getDocumentsCompressionConfigurationCommand;
