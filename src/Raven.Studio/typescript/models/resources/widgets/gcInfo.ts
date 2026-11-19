import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import genUtils = require("common/generalUtils");
import GcMemoryInfo = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("widgetsModels");

class gcInfo extends historyAwareNodeStats<GcInfoNormalizedData> {
    pauseFormatted = this.conditionalDataExtractor(x => x.memoryInfo.PauseTimePercentage.toFixed(2) + "%");
    pauseLevel = this.conditionalDataExtractor(x => {
        const percentage = x.memoryInfo.PauseTimePercentage;
        if (percentage >= 10) {
            return "danger";
        } else if (percentage >= 5) {
            return "warning";
        }
        
        return null;
    })
    type = this.conditionalDataExtractor(x => x.gcType);
    concurrent = this.conditionalDataExtractor(x => x.memoryInfo.Concurrent);
    compacted = this.conditionalDataExtractor(x => x.memoryInfo.Compacted);
    generation = this.conditionalDataExtractor(x => x.memoryInfo.Generation.toString()); 
    memoryFormatted = this.conditionalDataExtractor(
        x => genUtils.formatBytesToSize(x.memoryInfo.TotalHeapSizeAfterBytes), { customNoData: t("noData") });
    
    fragmentationInfo = ko.pureComputed(() => {
        const noData = this.noDataText();
        if (noData) {
            return null;
        }
        
        const fragmentationGen0 = this.gen0FragmentationFormatted();
        const fragmentationGen1 = this.gen1FragmentationFormatted();
        const fragmentationGen2 = this.gen2FragmentationFormatted();
        const fragmentationLoh = this.lohFragmentationFormatted();
        const fragmentationPoh = this.pinnedFragmentationFormatted();
        
        return `<div class="padding-xs">
                    <h3 class="text-center">${genUtils.escapeHtml(t("gcInfo.heapFragmentation"))}</h3>
                    <div class="details-item gen-0">
                        <div class="details-item-name">Gen0 <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationGen0}</div>
                    </div>
                    <div class="details-item gen-1">
                        <div class="details-item-name">Gen1 <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationGen1}</div>
                    </div>
                    <div class="details-item gen-2">
                        <div class="details-item-name">Gen2 <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationGen2}</div>
                    </div>
                    <div class="details-item loh">
                        <div class="details-item-name" title="${genUtils.escapeHtml(t("gcInfo.largeObjectHeap"))}">LOH <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationLoh}</div>
                    </div>
                    <div class="details-item pinned">
                        <div class="details-item-name" title="${genUtils.escapeHtml(t("gcInfo.pinnedObjectHeap"))}">POH <span class="rect"></span></div>
                        <div class="details-item-value">${fragmentationPoh}</div>
                    </div>
                </div>`
    })
    
    genProvider = (sizeAccessor: (memoryInfo: GcMemoryInfo) => Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize) => {
        return this.conditionalDataExtractor(x => {
            const size = sizeAccessor(x.memoryInfo);
            const before = genUtils.formatBytesToSize(size.SizeBeforeBytes);
            const after = genUtils.formatBytesToSize(size.SizeAfterBytes);
            return before + "→" + after;
        })
    }
    
    fragmentationProvider = (sizeAccessor: (memoryInfo: GcMemoryInfo) => Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize) => {
        return this.conditionalDataExtractor(x => {
            const size = sizeAccessor(x.memoryInfo);
            const before = genUtils.formatBytesToSize(size.FragmentationBeforeBytes);
            const after = genUtils.formatBytesToSize(size.FragmentationAfterBytes);
            return before + "→" + after;
        })
    }
    
    gen0Formatted = this.genProvider(x => x.Gen0HeapSize);
    gen1Formatted = this.genProvider(x => x.Gen1HeapSize);
    gen2Formatted = this.genProvider(x => x.Gen2HeapSize);
    lohFormatted = this.genProvider(x => x.LargeObjectHeapSize);
    pinnedFormatted = this.genProvider(x =>  x.PinnedObjectHeapSize);

    gen0FragmentationFormatted = this.fragmentationProvider(x => x.Gen0HeapSize);
    gen1FragmentationFormatted = this.fragmentationProvider(x => x.Gen1HeapSize);
    gen2FragmentationFormatted = this.fragmentationProvider(x => x.Gen2HeapSize);
    lohFragmentationFormatted = this.fragmentationProvider(x => x.LargeObjectHeapSize);
    pinnedFragmentationFormatted = this.fragmentationProvider(x =>  x.PinnedObjectHeapSize);
    
    pauseTotal = this.conditionalDataExtractor(x => {
        const pauses = x.memoryInfo.PauseDurationsInMs;
        const num = pauses.reduce((p, c) => p + c, 0);
        
        const showDistribution = pauses[1] > 0;
        const distribution = showDistribution ? " (" + gcInfo.formatGcPauseTime(pauses[0]) + "+" + gcInfo.formatGcPauseTime(pauses[1]) + ")" : "";
        return gcInfo.formatGcPauseTime(num) + distribution;
    })
    
    static formatGcPauseTime(millis: number) {
        if (millis > 1000 * 60) {
            const minutes = millis / 60_000;
            return t("gcInfo.pauseMinutes", { duration: genUtils.formatNumberToStringFixed(minutes, 2) });
        }
        return t("gcInfo.pauseMs", { duration: Math.round(millis) });
    }
    
    constructor(tag: string) {
        super(tag, "exact");
    }

    protected noDataText(): string|null {
        const currentItem = this.currentItem();
        return currentItem ? null : t("notAvailable");
    }
}


export = gcInfo;
