import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");
import reactUtils = require("common/reactUtils");
import DatabasesPage = require("components/pages/resources/databases/DatabasesPage");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getDatabasesMenuItem;

function getDatabasesMenuItem(appUrls: computedAppUrls) {
    const databasesView = reactUtils.bridgeToReact(DatabasesPage.DatabasesPage, "nonShardedView");
    
    appUrl.defaultModule = databasesView;
    
    return new leafMenuItem({
        route: "databases",
        title: t("databases.title"),
        moduleId: databasesView,
        nav: true,
        css: "icon-resources",
        dynamicHash: appUrls.databasesManagement,
        search: {
            innerActions: [
                { name: t("databases.addNew"), alternativeNames: [t("databases.create")] },
                { name: t("databases.restoreFromBackup") },
                { name: t("databases.disable") },
                { name: t("databases.enable") },
                { name: t("databases.pauseIndexingUntilRestart") },
                { name: t("databases.disableIndexing") },
                { name: t("databases.resumeIndexing") },
                { name: t("databases.restart") },
                { name: t("databases.compact") },
                { name: t("databases.delete"), alternativeNames: [t("databases.remove")] },
                { name: t("databases.allowDelete") },
                { name: t("databases.preventDelete") },
            ],
        },
    });
}
