import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getCountersCommand extends commandBase {

    constructor(private docId: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Counters.CountersDetail> {
        const args = {
            docId: this.docId,
            full: true
        };

        const url = endpoints.databases.counters.counters;

        return this.query<Raven.Client.Documents.Operations.Counters.CountersDetail>(url + this.urlEncodeArgs(args), null, this.db)
            .fail((result: JQueryXHR) => this.reportError(t("counters.getFailed"), result.responseText, result.statusText));
    }
 }

export = getCountersCommand; 
