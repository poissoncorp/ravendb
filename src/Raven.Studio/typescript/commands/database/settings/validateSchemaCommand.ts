import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class validateSchemaCommand extends commandBase {
    constructor(private db: database | string, private dto: ValidateSchemaRequestDto) {
        super();
    }

    execute(): JQueryPromise<ValidateSchemaResponseDto> {
        const url = endpoints.databases.schemaValidation.schemaValidationValidate;

        return this.post<ValidateSchemaResponseDto>(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError(t("validateSchemaFailed"), response.responseText, response.statusText)
            )
    }
}

export = validateSchemaCommand;
