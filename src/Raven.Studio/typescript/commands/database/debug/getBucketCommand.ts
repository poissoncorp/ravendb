import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getBucketCommand extends commandBase {
    
    constructor(private db: database | string, private bucket: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.Processors.BucketInfo> {
        const args = {
            bucket: this.bucket
        }
        const url = endpoints.databases.buckets.debugShardingBucket + this.urlEncodeArgs(args);
        return this.query<Raven.Server.Web.Studio.Processors.BucketInfo>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("loadBucketFailed"), response.responseText, response.statusText));
    }
}

export = getBucketCommand;
