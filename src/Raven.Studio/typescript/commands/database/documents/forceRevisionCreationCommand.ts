import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class forceRevisionCreationCommand extends commandBase {

    constructor(private id: string, private db: database | string, private reportSaveProgress: boolean = true) {
        super();
    }

    execute(): JQueryPromise<saveDocumentResponseDto> {
        const toBulkDoc: Partial<Raven.Server.Documents.Handlers.Batches.BatchRequestParser.CommandData> = {
            Id: this.id,
            Type: "ForceRevisionCreation"
        };
        
        const commands: Array<Partial<Raven.Server.Documents.Handlers.Batches.BatchRequestParser.CommandData>> = [ toBulkDoc ];

        const args = ko.toJSON({ Commands: commands });
        const url = endpoints.databases.batch.bulk_docs;
        
        const saveTask = this.post<saveDocumentResponseDto>(url, args, this.db);

        if (this.reportSaveProgress) {
            saveTask.done((result: saveDocumentResponseDto) => {
                if (result.Results[0].RevisionCreated) {
                    this.reportSuccess(t("revisionCreated", { id: result.Results[0]["@id"] }));
                }
                else {
                    this.reportSuccess(t("revisionNotCreated"));
                }
            });            
            saveTask.fail((response: JQueryXHR) => this.reportError(t("createRevisionFailed", { id: this.id }), response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = forceRevisionCreationCommand;
