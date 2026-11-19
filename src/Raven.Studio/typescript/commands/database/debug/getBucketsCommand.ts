import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getBucketsCommand extends commandBase {
    
    static expectedNumberOfRanges = 32;
    
    constructor(private db: database | string, private range?: { from: number, to: number }) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.Processors.BucketsResults> {
        const args = this.range ? {
            fromBucket: this.range.from,
            toBucket: this.range.to,
            range: (this.range.to - this.range.from + 1) / getBucketsCommand.expectedNumberOfRanges
        } : {};
        const url = endpoints.databases.buckets.debugShardingBuckets + this.urlEncodeArgs(args);
        return this.query<Raven.Server.Web.Studio.Processors.BucketsResults>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("loadBucketsReportFailed"), response.responseText, response.statusText));
    }
}

export = getBucketsCommand;
