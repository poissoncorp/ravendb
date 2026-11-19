import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getJsonSchemaFromSampleObjectCommand extends commandBase {
    constructor(
        private payload: object,
        private schemaType?: Raven.Server.Web.Studio.StudioTasksHandler.SchemaType
    ) {
        super();
    }

    execute(): JQueryPromise<{ Result: string }> {
        const args = {
            type: this.schemaType,
        };

        const url = endpoints.global.studioTasks.studioTasksConvertToJsonSchema + this.urlEncodeArgs(args);

        return this.post<{ Result: string }>(url, JSON.stringify(this.payload)).fail((response: JQueryXHR) => {
            this.reportError(
                t("jsonSchemaFailed"),
                response.responseText,
                response.statusText
            );
        });
    }
}

export = getJsonSchemaFromSampleObjectCommand;
