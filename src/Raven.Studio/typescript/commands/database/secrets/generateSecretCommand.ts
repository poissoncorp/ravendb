import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("secretsCommands");

class generateSecretCommand extends commandBase {

    execute(): JQueryPromise<string> {
        const url = endpoints.global.secretKey.secretsGenerate;

        return this.query<string>(url, null, null, null, { dataType: 'text' })
            .fail((response: JQueryXHR) => this.reportError(t("generateFailed"), response.responseText, response.statusText));
    }
}

export = generateSecretCommand;
