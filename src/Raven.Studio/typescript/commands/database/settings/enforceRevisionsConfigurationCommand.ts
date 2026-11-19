import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class enforceRevisionsConfigurationCommand extends commandBase {
    private readonly db: database | string;
    private readonly includeForceCreated: boolean;
    private readonly collections: string[];
    private readonly maxOpsPerSecond: number;

    constructor(db: database | string, includeForceCreated = false, collections: string[] = null, maxOpsPerSecond: number = null) {
        super();

        this.db = db;
        this.includeForceCreated = includeForceCreated;
        this.collections = collections;
        this.maxOpsPerSecond = maxOpsPerSecond;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.adminRevisions.adminRevisionsConfigEnforce;

        const args:  Raven.Client.Documents.Operations.Revisions.EnforceRevisionsConfigurationOperation.Parameters = {
            IncludeForceCreated: this.includeForceCreated,
            Collections: this.collections ?? undefined,
            ContinuationParameters: undefined,
            MaxOpsPerSecond: this.maxOpsPerSecond ?? undefined
        };

        return this.post<void>(url, JSON.stringify(args), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("enforceRevisionsConfigurationFailed"), response.responseText, response.statusText); 
            });
    }
}

export = enforceRevisionsConfigurationCommand; 

