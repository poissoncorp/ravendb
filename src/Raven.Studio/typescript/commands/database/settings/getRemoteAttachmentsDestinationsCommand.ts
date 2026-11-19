import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class getRemoteAttachmentsDestinationsCommand extends commandBase {
  constructor(private db: database | string) {
    super();
  }

  execute(): JQueryPromise<RemoteAttachmentsStudioConfiguration> {
    const url = endpoints.databases.studioDatabaseTasks.studioTasksRemoteAttachmentsConfiguration;

    const deferred = $.Deferred<RemoteAttachmentsStudioConfiguration>();
    this.query<RemoteAttachmentsStudioConfiguration>(url, null, this.db)
      .done((remoteAttachmentsConfig: RemoteAttachmentsStudioConfiguration) => deferred.resolve(remoteAttachmentsConfig))
      .fail((response: JQueryXHR) => {
        if (response.status === 404) {
          deferred.resolve(null);
        } else {
          deferred.reject(response);
          this.reportError(t("getRemoteAttachmentsDestinationsFailed"), response.responseText, response.statusText);
        }
      });

    return deferred;
  }
}

export = getRemoteAttachmentsDestinationsCommand;
