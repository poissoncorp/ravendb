import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class deleteServerWideConnectionStringCommand extends commandBase {
    constructor(
        private readonly type: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType,
        private readonly connectionStringName: string
    ) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            type: this.type,
            name: this.connectionStringName,
        };

        const url =
            endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings +
            this.urlEncodeArgs(args);

        return this.del<void>(url, null)
            .done(() =>
                this.reportSuccess(t("connectionStrings.deleteSucceeded", { name: this.connectionStringName }))
            )
            .fail((response: JQueryXHR) =>
                this.reportError(
                    t("connectionStrings.deleteFailed", { name: this.connectionStringName }),
                    response.responseText,
                    response.statusText
                )
            );
    }
}

export = deleteServerWideConnectionStringCommand;
