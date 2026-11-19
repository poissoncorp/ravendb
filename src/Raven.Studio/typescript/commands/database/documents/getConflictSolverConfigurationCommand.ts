import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getConflictSolverConfigurationCommand extends commandBase {
    private readonly db: database | string;

    constructor(db: database | string){
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.ConflictSolver> {

        const deferred = $.Deferred<Raven.Client.ServerWide.ConflictSolver>();
        const url = endpoints.databases.replication.replicationConflictsSolver;
        this.query(url, null, this.db)
            .done((expirationConfig: Raven.Client.ServerWide.ConflictSolver) => deferred.resolve(expirationConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError(t("getConflictSolverConfigurationFailed"), xhr.responseText, xhr.statusText);
                }
                
            });

        return deferred;
    }
}

export = getConflictSolverConfigurationCommand;
