import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class formatIndexCommand extends commandBase {

    constructor(private db: database | string, private expression: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.IndexMerging.SourceCodeBeautifier.FormattedExpression> {
        const url = endpoints.global.studioTasks.studioTasksFormat;
        const payload = {
            Expression: this.expression
        };
        return this.post(url, JSON.stringify(payload)).fail((response: JQueryXHR) => {
            this.reportError(t("formatFailed"), response.responseText, response.statusText);
        });
    }
}

export = formatIndexCommand;
