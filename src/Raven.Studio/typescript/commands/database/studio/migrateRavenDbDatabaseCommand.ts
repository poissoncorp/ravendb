import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import migrateRavenDbDatabaseModel = require("models/database/tasks/migrateRavenDbDatabaseModel");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("studioCommands");

class migrateRavenDbDatabaseCommand extends commandBase {

    constructor(private db: database | string, private model: migrateRavenDbDatabaseModel) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.smuggler.adminSmugglerMigrateRavendb;
        
        return this.post(url, JSON.stringify(this.model.toDto()), this.db)
            .fail((response: JQueryXHR) => this.reportError(t("migrateFailed"), response.responseText, response.statusText));
    }
}

export = migrateRavenDbDatabaseCommand; 
