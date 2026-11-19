import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");
import reactUtils = require("common/reactUtils");
import AboutPage = require("components/pages/resources/about/AboutPage");
import React = require("react");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

function aboutItem() {
    return new leafMenuItem({
        route: 'about',
        moduleId: reactUtils.bridgeToReact(AboutPage.AboutPage, "nonShardedView"),
        title: t("about.title"),
        tooltip: t("about.title"),
        nav: true,
        css: 'icon-info',
        dynamicHash: appUrl.forAbout,
        search: {
            innerActions: [
                {
                    name: t("about.license"),
                    alternativeNames: [
                        t("about.renewLicense"),
                        t("about.replaceLicense"),
                        t("about.upgradeLicense"),
                        t("about.forceUpdateLicense"),
                        t("about.licenseDetails"),
                    ],
                },
                { name: t("about.version"), alternativeNames: [t("about.serverVersion"), t("about.softwareVersion")] },
                { name: t("about.checkForUpdates") },
                { name: t("about.support") },
                { name: t("about.sendFeedback") },
            ],
        },
    });
}

interface WhatsNewItemOptions {
    isWhatsNewVisible?: boolean;
}

function whatsNewItem({ isWhatsNewVisible = false }: WhatsNewItemOptions = {}) {
    
    const moduleId = reactUtils.bridgeToReact(
        () => React.createElement(AboutPage.AboutPage, { initialChangeLogMode: "changeLog" }),
        "nonShardedView"
    );

    return new leafMenuItem({
        route: 'whatsNew',
        moduleId,
        title: t("releaseNotes"),
        tooltip: t("releaseNotes"),
        nav: isWhatsNewVisible,
        css: 'icon-document2',
        dynamicHash: appUrl.forWhatsNew,
        search: {
            isExcluded: !isWhatsNewVisible,
            isCapitalizedDisabled: true
        }
    });
}

function clusterDashboard() {
    const clusterDashboardView = require('viewmodels/resources/clusterDashboard');
    
    appUrl.clusterDashboardModule = clusterDashboardView;
    
    return new leafMenuItem({
        route: ["", "clusterDashboard"],
        moduleId: clusterDashboardView,
        title: t("clusterDashboard.title"),
        tooltip: t("clusterDashboard.title"),
        nav: true, // todo - this needs issue RavenDB-16618 to work...
        css: 'icon-cluster-dashboard',
        dynamicHash: appUrl.forClusterDashboard,
        search: {
            innerActions: [
                { name: t("clusterDashboard.addWidgets") },
                { name: t("clusterDashboard.deleteWidget"), alternativeNames: [t("clusterDashboard.removeWidget")] },
                { name: t("clusterDashboard.maximizeWidget") },
            ],
        },
    });
}

export = {
    about: aboutItem,
    whatsNew: whatsNewItem,
    clusterDashboard
};
