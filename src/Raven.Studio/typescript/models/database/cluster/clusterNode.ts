/// <reference path="../../../../typings/tsd.d.ts"/>
import clusterTopology = require("models/database/cluster/clusterTopology");
import generalUtils = require("common/generalUtils");
import license = require("models/auth/licenseModel");
import accessManager = require("common/shell/accessManager");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterModels");

class clusterNode {
    tag = ko.observable<string>();
    serverUrl = ko.observable<string>();
    type = ko.observable<clusterNodeType>();
    connected = ko.observable<boolean>();
    utilizedCores = ko.observable<number>();
    maxUtilizedCores = ko.observable<number | null>();
    numberOfCores = ko.observable<number>();
    installedMemoryInGb = ko.observable<number>();
    installedMemory = ko.pureComputed(() => this.formatNumber(this.installedMemoryInGb()));
    usableMemoryInGb = ko.observable<number>();
    usableMemory = ko.pureComputed(() => this.formatNumber(this.usableMemoryInGb()));
    errorDetails = ko.observable<string>();
    isLeader = ko.observable<boolean>();
    isPassive: KnockoutObservable<boolean>;
    nodeServerVersion = ko.observable<string>();
    osInfo = ko.observable<Raven.Client.ServerWide.Operations.OsInfo>();
    osFullName: KnockoutComputed<string>;
    osTitle: KnockoutComputed<string>;
    osIcon: KnockoutComputed<string>;
    memoryTitle: string;

    constructor(isPassive: KnockoutObservable<boolean>) {
        this.isPassive = isPassive;

        this.osFullName = ko.pureComputed(() => {
            const osInfo = this.osInfo();
            if (!osInfo) {
                return null;
            }

            let fullName = osInfo.FullName;
            if (!osInfo.Is64Bit) {
                fullName = t("node.osName32Bit", { name: fullName });
            }
            return fullName;
        });

        this.osTitle = ko.pureComputed(() => {
            const osInfo = this.osInfo();
            if (!osInfo) {
                return null;
            }

            let osTitle = `<div>${t("node.osTooltip.name", { name: generalUtils.escapeHtml(this.osFullName()) })}`;

            if (osInfo.Version) {
                osTitle += `<br />${t("node.osTooltip.version", { version: generalUtils.escapeHtml(osInfo.Version) })}`;
            }
            if (osInfo.BuildVersion) {
                osTitle += `<br />${t("node.osTooltip.buildVersion", {
                    context: osInfo.Type === "Linux" ? "kernel" : "build",
                    version: generalUtils.escapeHtml(osInfo.BuildVersion),
                })}`;
            }

            osTitle += "</div>";
            return osTitle;
        });
        
        this.osIcon = ko.pureComputed(() => clusterNode.osIcon(this.osInfo().Type));

        this.memoryTitle = t("node.memoryTooltip");
    }
    
    static osIcon(type: Raven.Client.ServerWide.Operations.OSType) {
        switch (type) {
            case "Linux":
                return "icon-linux";
            case "Windows":
                return "icon-windows";
            case "MacOS":
                return "icon-apple";
        }
    }
    
    errorDetailsShort = ko.pureComputed(() => {
        const longError = this.errorDetails();
        return generalUtils.trimMessage(longError);
    });

    utilizedMemoryInGb = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        const utilizedMemory = this.utilizedCores() * licenseStatus.Ratio;
        const installedMemoryInGb = this.installedMemoryInGb();
        return this.formatNumber(utilizedMemory ? Math.min(installedMemoryInGb, utilizedMemory) : installedMemoryInGb);
    });

    cssCores = ko.pureComputed(() => {
        if (this.utilizedCores() >= this.numberOfCores()) {
            return "text-success";
        }

        return "text-warning";
    });

    cssIcon = ko.pureComputed(() => {
        const type = this.type();
        switch (type) {
            case "Member":                
                if (this.isLeader()) {
                    return "icon-node-leader";
                } else {
                    return "icon-cluster-member";
                }
                
            case "Promotable":
                return "icon-cluster-promotable";
                
            case "Watcher":
                return "icon-cluster-watcher";
        }
    });

    updateWith(incoming: clusterNode) {
        this.tag(incoming.tag());
        this.type(incoming.type());
        this.connected(incoming.connected());
        this.utilizedCores(incoming.utilizedCores());
        this.maxUtilizedCores(incoming.maxUtilizedCores());
        this.numberOfCores(incoming.numberOfCores());
        this.installedMemoryInGb(incoming.installedMemoryInGb());
        this.usableMemoryInGb(incoming.usableMemoryInGb());
        this.errorDetails(incoming.errorDetails());
        this.isLeader(incoming.isLeader());
        this.nodeServerVersion(incoming.nodeServerVersion());
        this.osInfo(incoming.osInfo());
    }

    static for(tag: string, serverUrl: string, type: clusterNodeType, connected: boolean, isPassive: KnockoutObservable<boolean>, errorDetails?: string) {
        const node = new clusterNode(isPassive);
        node.tag(tag);
        node.serverUrl(serverUrl);
        node.type(type);
        node.connected(connected);
        node.errorDetails(errorDetails);
        return node;
    }

    createCanBePromotedObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();
            if (!topology.leader()) {
                return false;
            }
          
            return this.type() === "Watcher" &&
                   accessManager.default.isClusterAdminOrClusterNode();
        });
    }

    createCanBeDemotedObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();
            if (!topology.leader()) {
                return false;
            }
            
            return topology.leader() !== this.tag() && 
                   (this.type() === "Member" || this.type() === "Promotable") &&
                    accessManager.default.isClusterAdminOrClusterNode();
        });
    }

    createStateObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            const topology = topologyProvider();
            if (!topology.leader()) {
                if (this.type() === "Watcher") {
                    return t("node.state.waiting");
                } else if (this.isPassive()) {
                    return t("node.state.passive");
                } else {
                    return this.connected() ? t("node.state.voting") : t("node.state.error");
                }
            }

            return this.connected() ? t("node.state.active") : t("node.state.error");
        });
    }

    createStateClassObservable(topologyProvider: KnockoutObservable<clusterTopology>) {
        return ko.pureComputed(() => {
            
            const topology = topologyProvider();
            if (!topology.leader()) {
                if (this.type() === "Watcher") {
                    return "state-default";
                }
                
                return this.connected() ? "state-info" : "state-danger";
            }

            return this.connected() ? "state-success" : "state-danger";
        });
    }

    private formatNumber(num: number): string {
        return num.toFixed(1);
    }
}

export = clusterNode;
