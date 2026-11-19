import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class getSchemaValidationCommand extends commandBase {
    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<SchemaValidationConfiguration> {
        const url = endpoints.databases.schemaValidation.schemaValidationConfig;

        const deferred = $.Deferred<SchemaValidationConfiguration>();
        this.query<SchemaValidationConfiguration>(url, null, this.db)
            .done((schemaValidation: SchemaValidationConfiguration) => deferred.resolve(schemaValidation))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError(t("getSchemaValidationFailed"), xhr.responseText, xhr.statusText);
                }
            });

        return deferred;
    }
}

export = getSchemaValidationCommand;
