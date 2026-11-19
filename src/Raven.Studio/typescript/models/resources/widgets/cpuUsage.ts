import CpuUsagePayload = Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload;

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("widgetsModels");

class cpuUsage extends historyAwareNodeStats<CpuUsagePayload> {
    
    ravendbCoresInfo = this.conditionalDataExtractor(x => t("cpuUsage.utilizedCores", { utilized: x.UtilizedCores, count: x.NumberOfCores }), {
        customNoData: t("cpuUsage.utilizedCoresNoData")
    });

    machineCoresInfo = this.conditionalDataExtractor(x => t("cpuUsage.cores", { count: x.NumberOfCores }), {
        customNoData: t("cpuUsage.coresNoData")
    });
    
    processCpuUsageFormatted = this.conditionalDataExtractor(x => x.ProcessCpuUsage + "%");
    machineCpuUsageFormatted = this.conditionalDataExtractor(x => x.MachineCpuUsage + "%");
}


export = cpuUsage;
