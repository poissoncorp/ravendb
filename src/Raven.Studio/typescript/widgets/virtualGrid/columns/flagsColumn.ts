/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import document = require("models/database/documents/document");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("virtualGrid");

class flagsColumn implements virtualColumn {
    constructor(protected gridController: virtualGridController<any>) {
    }

    canHandle(): boolean {
        return false;
    }
    
    get sortable() {
        return false;
    }

    width = "100px";
    
    header = `<div style="padding-left: 8px;"><i class="icon-flag"></i></div>`;

    get headerAsText() {
        return t("columns.documentFlags");
    }

    renderCell(item: document): string {
        const metadata = item.__metadata;
        
        const extraClasses: string[] = [];
        let shardText: string = "";
        
        if (metadata) {
            const flags = (metadata.flags || "").split(",").map(x => x.trim());
            
            if (_.includes(flags, "HasAttachments")) {
                extraClasses.push("attachments");
            }
            if (_.includes(flags, "HasRevisions")) {
                extraClasses.push("revisions");
            }
            if (_.includes(flags, "HasCounters")) {
                extraClasses.push("counters");
            }
            if (_.includes(flags, "HasTimeSeries")) {
                extraClasses.push("time-series");
            }
            if (metadata.archived) {
                extraClasses.push("archived");
            }
            
            shardText = metadata.shardNumber != null ? '<span class="label label-default" title="' + generalUtils.escapeHtml(t("flags.shard")) + '">#' + metadata.shardNumber + '</span>' : "";
        }
        
        const attachmentsTitle = generalUtils.escapeHtml(t("flags.attachments"));
        const revisionsTitle = generalUtils.escapeHtml(t("flags.revisions"));
        const countersTitle = generalUtils.escapeHtml(t("flags.counters"));
        const timeSeriesTitle = generalUtils.escapeHtml(t("flags.timeSeries"));
        const archivedTitle = generalUtils.escapeHtml(t("flags.archived"));

        return `<div class="cell text-cell flags-cell ${extraClasses.join(" ")}" style="width: ${this.width}"><i title="${attachmentsTitle}" class="icon-attachment"></i><i title="${revisionsTitle}" class="icon-revisions"></i><i title="${countersTitle}" class="icon-new-counter"></i><i title="${timeSeriesTitle}" class="icon-new-time-series"></i><i title="${archivedTitle}" class="icon-data-archival"></i>${shardText}</div>`;
    }

    toDto(): virtualColumnDto {
        return {
            type: "flags",
            serializedValue: null,
            width: this.width,
            header: null
        }
    }

}

export = flagsColumn;
