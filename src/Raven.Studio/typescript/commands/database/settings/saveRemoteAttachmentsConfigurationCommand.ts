import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveRemoteAttachmentsConfigurationCommand extends commandBase {
  constructor(private db: database | string, private remoteAttachmentsConfiguration: RemoteAttachmentsConfiguration) {
    super();
  }

  execute(): JQueryPromise<void> {
    const url = endpoints.databases.remoteAttachment.adminAttachmentsRemoteConfig;

    return this.put<void>(url, JSON.stringify(this.remoteAttachmentsConfiguration), this.db)
      .fail((response: JQueryXHR) => this.reportError(t("saveRemoteAttachmentsConfigurationFailed"), response.responseText, response.statusText))
      .done(() => this.reportSuccess(t("saveRemoteAttachmentsConfigurationSucceeded")))
  }
}

export = saveRemoteAttachmentsConfigurationCommand;
