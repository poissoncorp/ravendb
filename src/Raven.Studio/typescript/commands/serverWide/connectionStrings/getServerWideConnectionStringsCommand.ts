import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import connectionStringsMapsFromDto = require("components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

interface GetServerWideConnectionStringsResult {
    Results: connectionStringsMapsFromDto.ServerWideConnectionStringDto[];
}

class getServerWideConnectionStringsCommand extends commandBase {
    execute(): JQueryPromise<GetServerWideConnectionStringsResult> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings;

        return this.query<GetServerWideConnectionStringsResult>(url, null, null)
            .fail((response: JQueryXHR) => {
                this.reportError(
                    t("connectionStrings.getFailed"),
                    response.responseText,
                    response.statusText
                );
            });
    }
}

export = getServerWideConnectionStringsCommand;
