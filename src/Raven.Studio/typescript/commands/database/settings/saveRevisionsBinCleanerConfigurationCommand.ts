import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveRevisionsBinCleanerConfigurationCommand extends commandBase {
  
  constructor(private db: database | string, private revisionsBinCleanerDto: RevisionsBinConfiguration) {
    super();
  }
  
  execute(): JQueryPromise<void> {
    const url = endpoints.databases.revisionsBinCleaner.adminRevisionsBinCleanerConfig;
    
    return this.post<void>(url, JSON.stringify(this.revisionsBinCleanerDto), this.db)
      .fail((response: JQueryXHR) => this.reportError(t("saveRevisionsBinCleanerConfigurationFailed"), response.responseText, response.statusText))
      .done(() => this.reportSuccess(t("saveRevisionsBinCleanerConfigurationSucceeded")))
  }
}

export = saveRevisionsBinCleanerConfigurationCommand;
