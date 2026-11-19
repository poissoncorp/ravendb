import commandBase from "commands/commandBase";
import database from "models/resources/database";
import endpoints from "endpoints";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("indexCommands");

export default class openFaultyIndexCommand extends commandBase {

    private indexNameToOpen: string;

    private db: database | string;

    private location: databaseLocationSpecifier;

    constructor(indexNameToOpen: string, db: database | string, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
        this.indexNameToOpen = indexNameToOpen;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexNameToOpen,
            ...this.location
        };

        const url = endpoints.databases.index.indexOpenFaultyIndex + this.urlEncodeArgs(args);
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(t("faultyIndexOpened", { indexName: this.indexNameToOpen })))
            .fail((response: JQueryXHR) => this.reportError(t("openFaultyIndexFailed", { indexName: this.indexNameToOpen }), response.responseText, response.statusText));
    }
}
