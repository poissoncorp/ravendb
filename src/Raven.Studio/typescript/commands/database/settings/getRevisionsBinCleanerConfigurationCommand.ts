import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RevisionsBinConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsBinConfiguration;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class getRevisionsBinCleanerConfigurationCommand extends commandBase {
  
  constructor(private db: database | string) {
    super();
  }
  
  execute(): JQueryPromise<RevisionsBinConfiguration> {
    const url = endpoints.databases.revisionsBinCleaner.revisionsBinCleanerConfig;
    
    const deferred = $.Deferred<RevisionsBinConfiguration>();
    this.query<RevisionsBinConfiguration>(url, null, this.db)
      .done((revisionBinCleanerConfig: RevisionsBinConfiguration) => deferred.resolve(revisionBinCleanerConfig))
      .fail((response: JQueryXHR) => {
        if (response.status === 404) {
          deferred.resolve(null);
        } else {
          deferred.reject(response);
          this.reportError(t("getRevisionsBinCleanerConfigurationFailed"), response.responseText, response.statusText);
        }
      });
    
    return deferred;
  }
}

export = getRevisionsBinCleanerConfigurationCommand;
