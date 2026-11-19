import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import collectionMenuItem = require("common/shell/menu/collectionMenuItem");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import DocumentIdentities = require("components/pages/database/documents/identities/DocumentIdentities");
import reactUtils = require("common/reactUtils");
import AllRevisions = require("components/pages/database/documents/allRevisions/AllRevisions");
import CompareExchange = require("components/pages/database/documents/compareExchange/CompareExchange");
import DocumentsPage = require("components/pages/database/documents/documentsList/DocumentsPage");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getDocumentsMenuItem;

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    const documentsItems = [
        new leafMenuItem({
            route: "databases/documents",
            moduleId: reactUtils.bridgeToReact(DocumentsPage.default, "shardedView"),
            shardingMode: "allShards",
            title: t("documents.allDocuments"),
            nav: false,
            css: "icon-documents",
            dynamicHash: appUrls.documents,
            search: {
                alternativeTitles: [t("documents.title")]
            }
        }),
        new leafMenuItem({
            route: "databases/documents/revisions/all",
            moduleId: reactUtils.bridgeToReact(AllRevisions.default, "shardedView"),
            shardingMode: "allShards",
            title: t("documents.allRevisions.title"),
            nav: false,
            css: "icon-revisions",
            dynamicHash: appUrls.allRevisions,
            search: {
                alternativeTitles: [t("documents.allRevisions.revisions")]
            }
        }),
        new leafMenuItem({
            route: "databases/documents/revisions/bin",
            moduleId: require("viewmodels/database/documents/revisionsBin"),
            shardingMode: "allShards",
            title: t("documents.revisionsBin"),
            nav: false,
            css: "icon-revisions-bin",
            dynamicHash: appUrls.revisionsBin,
        }),
        new collectionMenuItem(),
        new leafMenuItem({
            route: "databases/patch(/:recentPatchHash)",
            moduleId: require("viewmodels/database/patch/patch"),
            shardingMode: "allShards",
            title: t("documents.patch"),
            nav: true,
            css: "icon-patch",
            dynamicHash: appUrls.patch,
            requiredAccess: "DatabaseReadWrite"
        }),
        new leafMenuItem({
            route: "databases/query/index(/:indexNameOrRecentQueryIndex)",
            moduleId: require("viewmodels/database/query/query"),
            shardingMode: "allShards",
            title: t("documents.query"),
            nav: true,
            css: "icon-documents-query",
            alias: true,
            dynamicHash: appUrls.query(""),
            search: {
                isExcluded: true, // it is also defined in indexes menu
            }
        }),
        new leafMenuItem({
            route: "databases/edit",
            moduleId: require("viewmodels/database/documents/editDocument"),
            shardingMode: "allShards",
            title: t("documents.editDocument.title"),
            nav: false,
            css: "icon-new-document",
            itemRouteToHighlight: "databases/documents",
            dynamicHash: appUrls.newDoc,
            search: {
                overrideTitle: t("documents.editDocument.addNew"),
                alternativeTitles: [t("documents.editDocument.create")],
            }
        }),
        new leafMenuItem({
            route: "databases/ts/edit",
            moduleId: require("viewmodels/database/timeSeries/editTimeSeries"),
            title: t("documents.editTimeSeries"),
            nav: false,
            itemRouteToHighlight: "databases/documents",
            search: {
                isExcluded: true
            }
        }),
        new leafMenuItem({
            route: "databases/identities",
            moduleId: reactUtils.bridgeToReact(DocumentIdentities.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("documents.identities.title"),
            nav: true,
            css: "icon-identities",
            dynamicHash: appUrls.identities,
            search: {
                innerActions: [
                    { name: t("documents.identities.addNew"), alternativeNames: [t("documents.identities.create")] }
                ],
            },
        }),
        new leafMenuItem({
            route: "databases/cmpXchg",
            moduleId: reactUtils.bridgeToReact(CompareExchange.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("documents.compareExchange"),
            nav: true,
            css: "icon-cmp-xchg",
            dynamicHash: appUrls.cmpXchg
        }),
        new leafMenuItem({
            route: "databases/cmpXchg/edit",
            moduleId: require("viewmodels/database/cmpXchg/editCmpXchg"),
            shardingMode: "allShards",
            title: t("documents.editCompareExchange.title"),
            nav: false,
            css: "icon-plus",
            itemRouteToHighlight: "databases/cmpXchg",
            dynamicHash: appUrls.cmpXchg,
            search: {
                overrideTitle: t("documents.editCompareExchange.addNew"),
                alternativeTitles: [t("documents.editCompareExchange.create")],
            }
        }),
        new leafMenuItem({
            route: "databases/documents/conflicts",
            moduleId: require("viewmodels/database/conflicts/conflicts"),
            shardingMode: "allShards",
            title: t("documents.conflicts"),
            nav: true,
            css: "icon-conflicts",
            dynamicHash: appUrls.conflicts,
            badgeData: collectionsTracker.default.conflictsCount
        })
    ];

    return new intermediateMenuItem(t("documents.title"), documentsItems, "icon-documents", {
        dynamicHash: appUrls.documents
    });
}
