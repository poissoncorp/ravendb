import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class setLicenseLimitsCommand extends commandBase {

    constructor(private nodeTag: string, private maxUtilizedCores: number | null) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag,
            maxUtilizedCores : this.maxUtilizedCores
        }

        const url = endpoints.global.rachisAdmin.adminLicenseSetLimit + this.urlEncodeArgs(args);

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError(t("setLicenseLimitsFailed"), response.responseText, response.statusText);
            })
            .done(() => this.reportSuccess(t("setLicenseLimitsSucceeded")));
    }
}

export = setLicenseLimitsCommand; 
