import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import footer = require("common/shell/footer");
import reactUtils = require("common/reactUtils");
import IndexesPage = require("components/pages/database/indexes/list/IndexesPage");
import IndexCleanup = require("components/pages/database/indexes/cleanup/IndexCleanup");
import IndexErrors = require("components/pages/database/indexes/errors/IndexErrors");
import IndexTerms = require("components/pages/database/indexes/list/terms/IndexTerms");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getIndexesMenuItem;

function getIndexesMenuItem(appUrls: computedAppUrls) {
    const indexesItems = [
        new leafMenuItem({
            route: 'databases/query/index(/:indexNameOrRecentQueryIndex)',
            shardingMode: "allShards",
            moduleId: require('viewmodels/database/query/query'),
            title: t("indexes.query"),
            nav: true,
            css: 'icon-indexes-query',
            dynamicHash: appUrls.query('')
        }),
        new leafMenuItem({
            title: t("indexes.listOfIndexes.title"),
            nav: true,
            shardingMode: "allShards",
            route: "databases/indexes",
            moduleId: reactUtils.bridgeToReact(IndexesPage.IndexesPage, "shardedView"),
            css: 'icon-list-of-indexes',
            dynamicHash: appUrls.indexes(null, false, false),
            search: {
                innerActions: [
                    { name: t("indexes.listOfIndexes.setPriority"), alternativeNames: [t("indexes.listOfIndexes.setLowPriority"), t("indexes.listOfIndexes.setNormalPriority"), t("indexes.listOfIndexes.setHighPriority")] },
                    { name: t("indexes.listOfIndexes.setLockMode"), alternativeNames: [t("indexes.listOfIndexes.unlockIndex"), t("indexes.listOfIndexes.lockIndex")] },
                    { name: t("indexes.listOfIndexes.setState"), alternativeNames: [t("indexes.listOfIndexes.startIndexing"), t("indexes.listOfIndexes.disableIndexing"), t("indexes.listOfIndexes.pauseIndexingUntilRestart")] },
                    { name: t("indexes.listOfIndexes.deleteIndex"), alternativeNames: [t("indexes.listOfIndexes.removeIndex")] },
                    { name: t("indexes.listOfIndexes.editIndex") },
                    { name: t("indexes.listOfIndexes.resetIndex"), alternativeNames: [t("indexes.listOfIndexes.resetInPlace"), t("indexes.listOfIndexes.resetSideBySide")] }, 
                    { name: t("indexes.listOfIndexes.exportIndexes") }
                ],
            },
        }),
        new leafMenuItem({
            title: t("indexes.listOfIndexes.title"),
            nav: false,
            shardingMode: "allShards",
            route: "databases/indexes",
            moduleId: reactUtils.bridgeToReact(IndexesPage.IndexesPage, "shardedView"),
            css: 'icon-index-import',
            itemRouteToHighlight: 'databases/indexes',
            dynamicHash: appUrls.indexes(null, false, true),
            search: {
                overrideTitle: t("indexes.listOfIndexes.importIndexes"),
            }
        }),
        new leafMenuItem({
            route: 'databases/indexes/performance',
            shardingMode: "singleShard",
            moduleId: require('viewmodels/database/indexes/indexPerformance'),
            title: t("indexes.indexingPerformance.title"),
            tooltip: t("indexes.indexingPerformance.tooltip"),
            nav: true,
            css: 'icon-indexing-performance',
            dynamicHash: appUrls.indexPerformance,
            search: {
                innerActions: [
                    { name: t("indexes.indexingPerformance.exportPerformance") },
                    { name: t("indexes.indexingPerformance.importPerformance") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/indexes/visualizer',
            shardingMode: "singleShard",
            moduleId: require('viewmodels/database/indexes/visualizer/visualizer'),
            title: t("indexes.mapReduceVisualizer"),
            nav: true,
            css: 'icon-map-reduce-visualizer',
            dynamicHash: appUrls.visualizer
        }),
        new leafMenuItem({
            route: 'databases/indexes/cleanup',
            moduleId: reactUtils.bridgeToReact(IndexCleanup.IndexCleanup, "nonShardedView"),
            shardingMode: "allShards",
            title: t("indexes.indexCleanup.title"),
            nav: true,
            css: 'icon-index-cleanup',
            dynamicHash: appUrls.indexCleanup,
            search: {
                innerActions: [
                    { name: t("indexes.indexCleanup.mergeIndexes") },
                    { name: t("indexes.indexCleanup.removeSubIndexes"), alternativeNames: [t("indexes.indexCleanup.deleteSubIndexes")] },
                    { name: t("indexes.indexCleanup.removeUnusedIndexes"), alternativeNames: [t("indexes.indexCleanup.deleteUnusedIndexes")] },
                    { name: t("indexes.indexCleanup.unmergableIndexes") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/indexes/indexErrors',
            shardingMode: "allShards",
            moduleId: reactUtils.bridgeToReact(IndexErrors.default, "nonShardedView"),
            title: t("indexes.indexErrors"),
            nav: true,
            css: 'icon-index-errors',
            dynamicHash: appUrls.indexErrors,
            badgeData: ko.pureComputed(() => { return footer.default.stats() ? footer.default.stats().countOfIndexingErrors() : null; })
        }),
        new leafMenuItem({
            title: t("indexes.addNewIndex.title"),
            shardingMode: "allShards",
            route: 'databases/indexes/new',
            moduleId: require('viewmodels/database/indexes/editIndex'),
            css: 'icon-plus',
            nav: false,
            itemRouteToHighlight: 'databases/indexes',
            dynamicHash: appUrls.newIndex,
            search: {
                alternativeTitles: [t("indexes.addNewIndex.createIndex")],
            }
        }),
        new leafMenuItem({
            title: t("indexes.editIndex"),
            shardingMode: "allShards",
            route: 'databases/indexes/edit(/:indexName)',
            moduleId: require('viewmodels/database/indexes/editIndex'),
            css: 'icon-edit',
            nav: false,
            itemRouteToHighlight: 'databases/indexes',
            search: {
                isExcluded: true
            }
        }),
        new leafMenuItem({
            title: t("indexes.terms"),
            route: 'databases/indexes/terms/(:indexName)',
            moduleId: reactUtils.bridgeToReact(IndexTerms.default, "shardedView"),
            shardingMode: "allShards",
            css: 'icon-terms',
            nav: false,
            search: {
                isExcluded: true
            }
        })
    ];

    return new intermediateMenuItem(t("indexes.title"), indexesItems, 'icon-indexing', {
        dynamicHash: appUrls.indexes(null, false, false)
    });
}
