import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("patchCommands");

interface patchCommandOptions {
    test?: boolean;
    documentId?: string;
    allowStale?: boolean;
    staleTimeout?: string;
    maxOpsPerSecond?: number;
    disableAutoIndexCreation?: boolean;
    ignoreMaxStepsForScript?: boolean;
}

class patchCommand extends commandBase {

    private static readonly missingUpdateClause = "Update operations must end with UPDATE clause";
    
    constructor(private queryStr: string, private db: database | string, private options: patchCommandOptions = null) {
        super();
                
        this.options = this.options || {
            test: false,
            documentId: null
        };
    }

    execute(): JQueryPromise<operationIdDto> {

        const args = {
            allowStale: this.options.allowStale,
            staleTimeout: this.options.staleTimeout,
            maxOpsPerSec: this.options.maxOpsPerSecond,
            id: this.options.test ? this.options.documentId : undefined,
            disableAutoIndexCreation: this.options.disableAutoIndexCreation,
            ignoreMaxStepsForScript: this.options.ignoreMaxStepsForScript ? true : undefined
        };

        const payload = {
            Query: {
                Query: this.queryStr
            }
        };
        
        const url = this.options.test ?
            endpoints.databases.queries.queriesTest :
            endpoints.databases.queries.queries;
            
        return this.patch<operationIdDto>(url + this.urlEncodeArgs(args), JSON.stringify(payload), this.db)
            .done((response: operationIdDto) => {
                if (!this.options.test) {
                    this.reportSuccess(t("scheduled"));
                }
                
                return response;
            })
            .fail((response: JQueryXHR) => {
                const responseText = response.responseText;
                let errorTitle = t("failed", { context: this.options.test ? "test" : "schedule" });
                
                if (responseText.includes(patchCommand.missingUpdateClause)) {
                    errorTitle = t("incorrectSyntax");
                }
               
                this.reportError(errorTitle, responseText, response.statusText);
            });
    }
}

export = patchCommand; 
