import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import RevertRevisionsRequest = Raven.Server.Documents.Revisions.RevertRevisionsRequest;

const t = i18nModule.createTranslator("documentsCommands");

class revertRevisionsCommand extends commandBase {
    private readonly payload: RevertRevisionsRequest;
    private readonly db: database | string;

    constructor(payload: RevertRevisionsRequest, db: database | string) {
        super();
        this.db = db;
        this.payload = payload;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.revisions.revisionsRevert;

        return this.post(url, JSON.stringify(this.payload), this.db).fail((response: JQueryXHR) =>
            this.reportError(t("revertRevisionsFailed"), response.responseText)
        );
    }
}

export = revertRevisionsCommand;
