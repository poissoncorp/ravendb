import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import reactUtils = require("common/reactUtils");
import StatisticsPage = require("components/pages/database/status/statistics/StatisticsPage");
import TombstonesState = require("components/pages/database/status/tombstones/TombstonesState");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getStatsMenuItem;

function getStatsMenuItem(appUrls: computedAppUrls, isSharded: boolean) {
    const statsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/status',
            moduleId: reactUtils.bridgeToReact(StatisticsPage.StatisticsPage, "nonShardedView"),
            shardingMode: "allShards",
            title: t("stats.title"),
            nav: true,
            css: 'icon-stats',
            dynamicHash: appUrls.status
        }),
        new leafMenuItem({
            route: 'databases/status/ioStats',
            moduleId: require('viewmodels/database/status/ioStats'),
            shardingMode: "singleShard",
            title: t("stats.ioStats.title"),
            tooltip: t("stats.ioStats.tooltip"),
            nav: true,
            css: 'icon-io-test',
            dynamicHash: appUrls.ioStats,
            search: {
                innerActions: [
                    { name: t("stats.ioStats.export") },
                    { name: t("stats.ioStats.import") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/status/storage/report',
            moduleId: require('viewmodels/database/status/storageReport'),
            shardingMode: "singleShard",
            title: t("stats.storageReport"),
            tooltip: t("stats.storageReport"),
            nav: true,
            css: 'icon-storage',
            dynamicHash: appUrls.statusStorageReport
        }),
        new leafMenuItem({
            route: 'databases/status/buckets/report',
            moduleId: require('viewmodels/database/status/bucketsReport'),
            shardingMode: "allShards",
            title: t("stats.bucketsReport"),
            tooltip: t("stats.bucketsReport"),
            nav: isSharded,
            css: 'icon-storage', //TODO:
            dynamicHash: appUrls.statusBucketsReport,
            search: {
                isExcluded: !isSharded
            }
        }),
        new leafMenuItem({
            route: "virtual", // here we only redirect to global section with proper db set in url
            moduleId: () => { /* empty */},
            title: t("stats.runningQueries"),
            nav: true,
            css: 'icon-stats-running-queries',
            dynamicHash: appUrls.runningQueries
        }),
        new leafMenuItem({
            route: 'databases/status/ongoingTasksStats',
            moduleId: require('viewmodels/database/status/ongoingTasksStats'),
            shardingMode: "singleShard",
            title: t("stats.ongoingTaskStats.title"),
            nav: true,
            css: 'icon-replication-stats',
            dynamicHash: appUrls.ongoingTasksStats,
            search: {
                innerActions: [
                    { name: t("stats.ongoingTaskStats.export") },
                    { name: t("stats.ongoingTaskStats.import") },
                ],
            },
        }),
        new separatorMenuItem(),
        new separatorMenuItem(t("stats.advanced")),
        new leafMenuItem({
            route: 'databases/status/tombstonesState',
            moduleId: reactUtils.bridgeToReact(TombstonesState.default, "shardedView"),
            title: t("stats.tombstones.title"),
            nav: true,
            shardingMode: "singleShard",
            css: 'icon-tombstones',
            dynamicHash: appUrls.tombstonesState,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("stats.tombstones.forceCleanup") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/status/debug*details',
            moduleId: null,
            title: t("stats.debug"),
            nav: false,
            css: 'icon-debug',
            search: {
                isExcluded: true
            }
        })
    ];

    return new intermediateMenuItem(t("stats.title"), statsItems, "icon-stats-menu");
}
