import viewModelBase = require("viewmodels/viewModelBase");
import removeNodeFromClusterCommand = require("commands/database/cluster/removeNodeFromClusterCommand");
import leaderStepDownCommand = require("commands/database/cluster/leaderStepDownCommand");
import promoteClusterNodeCommand = require("commands/database/cluster/promoteClusterNodeCommand");
import demoteClusterNodeCommand = require("commands/database/cluster/demoteClusterNodeCommand");
import bootstrapClusterCommand = require("commands/database/cluster/bootstrapClusterCommand");
import forceLeaderTimeoutCommand = require("commands/database/cluster/forceLeaderTimeoutCommand");
import changesContext = require("common/changesContext");
import clusterNode = require("models/database/cluster/clusterNode");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import showDataDialog = require("viewmodels/common/showDataDialog");
import router = require("plugins/router");
import clusterGraph = require("models/database/cluster/clusterGraph");
import assignCores = require("viewmodels/manage/assignCores");
import license = require("models/auth/licenseModel");
import eventsCollector = require("common/eventsCollector");
import accessManager = require("common/shell/accessManager");
import generalUtils = require("common/generalUtils");
import typeUtils = require("common/typeUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("manageCluster");
const tCommon = i18nModule.createTranslator("common");

class cluster extends viewModelBase {

    view = require("views/manage/cluster.html");

    private graph = new clusterGraph();

    topology = clusterTopologyManager.default.topology;
    accessManager = accessManager.default;
    aboutViewUrl = appUrl.forAbout();

    passiveStateHtml = t("passive.state");
    bootstrapOptionsHtml = t("passive.bootstrapOptions");
    registerLicenseHtml = t("passive.registerLicense", { aboutViewUrl: generalUtils.escapeHtml(this.aboutViewUrl) });
    assignedCoresLimitTitle = t("node.assignedCoresLimitTitle");
    totalCoresTitle = t("node.totalCoresTitle");
    installedMemoryTitle = t("node.installedMemoryTitle");

    canDeleteNodes: KnockoutComputed<boolean>;
    canAddNodes: KnockoutComputed<boolean>;
    canBootstrapCluster: KnockoutComputed<boolean>;
    canStepDown: KnockoutComputed<boolean>;
    
    leaderUrl: KnockoutComputed<string>;
    utilizedCores: KnockoutComputed<number>;
    maxCores: KnockoutComputed<number>;
    totalServersCores: KnockoutComputed<number>;

    clientVersion = viewModelBase.clientVersion;

    clusterDebugUrl: KnockoutComputed<string>;

    cssCores = ko.pureComputed(() => {
        if (this.utilizedCores() === this.maxCores()) {
            return "text-success";
        }

        return "text-warning";
    });

    spinners = {
        stepdown: ko.observable<boolean>(false),
        delete: ko.observableArray<string>([]),
        promote: ko.observableArray<string>([]),
        demote: ko.observableArray<string>([]),
        forceTimeout: ko.observable<boolean>(false),
        assignCores: ko.observable<boolean>(false),
        bootstrap: ko.observable<boolean>(false)
    };

    constructor() {
        super();
        this.bindToCurrentInstance("deleteNode", "stepDown", "promote", "demote", "forceTimeout", "showErrorDetails", "assignCores");

        this.initObservables();
    }

    compositionComplete() {
        super.compositionComplete();

        $('.cluster [data-toggle="tooltip"]').tooltip();

        this.graph.init($("#clusterGraphContainer"), this.topology().nodes().length);

        this.graph.draw(this.topology().nodes(), this.topology().leader(), this.topology().isPassive());

        const serverWideClient = changesContext.default.serverNotifications();

        this.addNotification(serverWideClient.watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(serverWideClient.watchReconnect(() => this.refresh()));
    }

    deactivate():void {
        super.deactivate();
        this.graph.dispose();
    }

    private initObservables() {
        this.canDeleteNodes = ko.pureComputed(() => this.topology().leader() && this.topology().nodes().length > 1);
        this.canAddNodes = ko.pureComputed(() => !!this.topology().leader() || this.topology().nodeTag() === "?");
        this.canBootstrapCluster = ko.pureComputed(() => this.topology().nodeTag() === "?");
        this.canStepDown = ko.pureComputed(() => this.topology().membersCount() >= 2);
        
        this.leaderUrl = ko.pureComputed(() => {
            const topology = this.topology();

            if (!topology.leader()) {
                return "";
            }

            const leaderNode = topology.nodes().find(x => x.tag() === topology.leader());
            
            if (!leaderNode) {
                return "";
            }
            
            const serverUrl = leaderNode.serverUrl();
            const localPart = appUrl.forCluster();

            return appUrl.toExternalUrl(serverUrl, localPart);
        });

        this.utilizedCores = ko.pureComputed(() => {
            const nodes = this.topology().nodes();
            const utilizedCores = typeUtils.sumBy(nodes, x => !x.utilizedCores() ? 0 : x.utilizedCores());
            return utilizedCores;
        });

        this.maxCores = ko.pureComputed(() => {
            const status = license.licenseStatus();
            if (!status) {
                return -1;
            }

            return status.MaxCores;
        });

        this.totalServersCores = ko.pureComputed(() => {
            const nodes = this.topology().nodes();
            const numberOfCores = typeUtils.sumBy(nodes, x => !x.numberOfCores() || x.numberOfCores() === -1 ? 0 : x.numberOfCores());
            return numberOfCores;
        });

        this.clusterDebugUrl = ko.pureComputed(() => {
            return appUrl.forDebugAdvancedClusterDebug();
        })
    }

    activate(args: any) {
        super.activate(args);

        this.updateHelpLink("11HBHO");
    }

    serverVersionTitle(version: string) {
        return t("node.versionTitle", { version });
    }

    nodeTypeLabel(type: clusterNodeType) {
        switch (type) {
            case "Member":
                return t("nodeTypes.member");
            case "Promotable":
                return t("nodeTypes.promotable");
            case "Watcher":
                return t("nodeTypes.watcher");
            default:
                return type;
        }
    }

    promote(node: clusterNode) {
        this.confirmationMessage(t("confirm.title"), t("confirm.promote"), {
            buttons: [tCommon("cancel"), t("confirm.promoteButton")]
        })
            .done(result => {
               if (result.can) {
                   eventsCollector.default.reportEvent("cluster", "promote");
                   this.spinners.promote.push(node.tag());
                   new promoteClusterNodeCommand(node.tag())
                       .execute()
                       .always(() => this.spinners.promote.remove(node.tag()));
               } 
            });
    }

    demote(node: clusterNode) {
         this.confirmationMessage(t("confirm.title"), t("confirm.demote"), {
             buttons: [tCommon("cancel"), t("confirm.demoteButton")]
         })
            .done(result => {
               if (result.can) {
                   eventsCollector.default.reportEvent("cluster", "demote");
                   this.spinners.demote.push(node.tag());
                   new demoteClusterNodeCommand(node.tag())
                       .execute()
                       .always(() => this.spinners.demote.remove(node.tag()));
               } 
            });
    }

    stepDown() {
        this.confirmationMessage(t("confirm.title"), t("confirm.stepDown"), {
            buttons: [tCommon("cancel"), t("confirm.stepDownButton")]
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("cluster", "step-down");
                    this.spinners.stepdown(true);
                    new leaderStepDownCommand()
                        .execute()
                        .always(() => this.spinners.stepdown(false));
                }
            });
    }

    deleteNode(node: clusterNode) {
        this.confirmationMessage(t("confirm.title"), t("confirm.remove", { serverUrl: generalUtils.escapeHtml(node.serverUrl()) }),{
            buttons: [tCommon("cancel"), t("confirm.removeButton")],
            html: true
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("cluster", "delete-node");
                    this.spinners.delete.push(node.tag());
                    new removeNodeFromClusterCommand(node.tag())
                        .execute()
                        .always(() => this.spinners.delete.remove(node.tag()));
                }
            });
    }

    addNode() {
        router.navigate(appUrl.forAddClusterNode());
    }

    bootstrapCluster() {
        this.spinners.bootstrap(true);    
        new bootstrapClusterCommand()
            .execute()
            .always(() => this.spinners.bootstrap(false));
    }

    forceTimeout(node: clusterNode) {
        this.confirmationMessage(t("confirm.title"), t("confirm.forceTimeout"), {
            buttons: [tCommon("cancel"), t("confirm.forceTimeoutButton")]
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("cluster", "timeout");
                    this.spinners.forceTimeout(true);
                    new forceLeaderTimeoutCommand(node.serverUrl())
                        .execute()
                        .always(() => this.spinners.forceTimeout(false));
                }
            });
    }

    assignCores(node: clusterNode) {
        const utilizedCores = typeUtils.sumBy(this.topology().nodes(), x => x.utilizedCores());
        const availableCores = license.licenseStatus().MaxCores - utilizedCores;
        const assignCoresView = new assignCores(node.tag(), node.utilizedCores(), node.maxUtilizedCores(), availableCores, node.numberOfCores());
        app.showBootstrapDialog(assignCoresView);
    }

    private refresh() {
        $('.cluster [data-toggle="tooltip"]').tooltip();
        this.graph.draw(this.topology().nodes(), this.topology().leader(), this.topology().isPassive());
    }

    showErrorDetails(tag: string) {
        const node = this.topology().nodes().find(x => x.tag() === tag);

        app.showBootstrapDialog(new showDataDialog(t("node.errorDetailsTitle", { nodeTag: tag }), node.errorDetails(), "plain"));
    }
}

export = cluster;
