import generalUtils = require("common/generalUtils");
import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("widgetsModels");

class serverTraffic extends historyAwareNodeStats<Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload> {

    attachmentsWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.AttachmentsWriteBytesPerSecond) + "/s", { customNoData: t("notAvailable") });
    attachmentWritesPerSecond = this.conditionalDataExtractor(x => x.AttachmentWritesPerSecond.toLocaleString() + "/s", { customNoData: t("notAvailable") });
    countersWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.CountersWriteBytesPerSecond) + "/s", { customNoData: t("notAvailable") });
    counterWritesPerSecond = this.conditionalDataExtractor(x => x.CounterWritesPerSecond.toLocaleString() + "/s", { customNoData: t("notAvailable") });
    documentsWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.DocumentsWriteBytesPerSecond) + "/s", { customNoData: t("notAvailable") });
    documentWritesPerSecond = this.conditionalDataExtractor(x => x.DocumentWritesPerSecond.toLocaleString() + "/s", { customNoData: t("notAvailable") });
    requestsPerSecond = this.dataExtractor(x => x.RequestsPerSecond);
    avgRequestTime = this.dataExtractor(x => x.AverageRequestDuration);
    timeSeriesWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.TimeSeriesWriteBytesPerSecond) + "/s", { customNoData: t("notAvailable") });
    timeSeriesWritesPerSecond = this.conditionalDataExtractor(x => x.TimeSeriesWritesPerSecond.toLocaleString() + "/s", { customNoData: t("notAvailable") });
    
    totalWritesPerSecond = this.dataExtractor(x => x.DocumentWritesPerSecond
        + x.AttachmentWritesPerSecond
        + x.CounterWritesPerSecond
        + x.TimeSeriesWritesPerSecond);
    
    totalWritesPerSecondSize = this.conditionalDataExtractor(() => this.totalWritesPerSecond().toLocaleString(), { customNoData: "-" });
    
    totalWriteBytesPerSecond = this.dataExtractor(x => x.DocumentsWriteBytesPerSecond
        + x.AttachmentsWriteBytesPerSecond
        + x.CountersWriteBytesPerSecond
        + x.TimeSeriesWriteBytesPerSecond);

    totalWriteBytesPerSecondSize = this.conditionalDataExtractor(() => generalUtils.formatBytesToSize(this.totalWriteBytesPerSecond(), 2, true)[0]);
    totalWriteBytesPerSecondUnit = this.conditionalDataExtractor(() => generalUtils.formatBytesToSize(this.totalWriteBytesPerSecond(), 2, true)[1], { customNoData: "-" });
    
    requestsFormatted: KnockoutComputed<string>;
    avgRequestTimeFormatted: KnockoutComputed<string>;
    
    constructor(tag: string) {
        super(tag);
        
        this.requestsFormatted = ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return noData;
            }
            return this.requestsPerSecond().toLocaleString();
        });
        
        this.avgRequestTimeFormatted = ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return noData;
            }
            return Math.round(this.avgRequestTime()).toLocaleString(); 
        });
    }
}

export = serverTraffic;
