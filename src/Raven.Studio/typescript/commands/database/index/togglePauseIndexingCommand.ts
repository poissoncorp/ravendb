import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

type optsNames = {
    name: string;
}

type toggleType = "map" | "map-reduce";

type optsType = {
    type: toggleType;
}

class togglePauseIndexingCommand extends commandBase {

    private toggleAll = false;
    private readonly name: string;
    private readonly type: toggleType;
    private readonly location: databaseLocationSpecifier;

    private readonly start: boolean;

    private readonly db: database | string;

    constructor(start: boolean, db: database | string, options: optsNames | optsType = null, location: databaseLocationSpecifier = undefined) {
        super();
        this.db = db;
        this.start = start;

        this.location = location;
        
        if (options && "name" in options) {
            this.name = options.name;
        } else if (options && "type" in options) {
            this.type = options.type;
        } else {
            this.toggleAll = true;
        }
    }

    execute(): JQueryPromise<void> {
        const basicUrl = this.start ? endpoints.databases.adminIndex.adminIndexesStart : endpoints.databases.adminIndex.adminIndexesStop;

        const args: any = {
            ...this.location
        };
        if (this.name) {
            args.name = this.name;
        } else if (this.type) {
            args.type = this.type;
        }

        const url = basicUrl + (args ? this.urlEncodeArgs(args) : "");
        

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(this.getFailTitle(), response.responseText));
    }

    getFailTitle() {
        const locationText = genUtils.formatLocation(this.location);
        if (this.name) {
            const options = { indexName: this.name, location: locationText };
            return this.start ? t("resumeIndexFailed", options) : t("pauseIndexFailed", options);
        }

        return this.start
            ? t("resumeAllFailed", { location: locationText })
            : t("pauseAllFailed", { location: locationText });
    }
}

export = togglePauseIndexingCommand;
