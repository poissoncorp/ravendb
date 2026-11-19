import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import connectionStringsTypes = require("components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class saveServerWideConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: connectionStringsTypes.ServerWideConnectionStringDto) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings;

        return this.put<void>(url, JSON.stringify(this.connectionString))
            .fail((response: JQueryXHR) =>
                this.reportError(
                    t("connectionStrings.saveFailed"),
                    response.responseText,
                    response.statusText
                )
            )
            .done(() => this.reportSuccess(t("connectionStrings.saveSucceeded")));
    }
}

export = saveServerWideConnectionStringCommand;
