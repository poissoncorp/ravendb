import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import reactUtils = require("common/reactUtils");
import AiConnectionStrings = require("components/pages/database/aiHub/aiConnectionStrings/AiConnectionStrings");
import AiTasks = require("components/pages/database/aiHub/aiTasks/AiTasks");
import AiAgents = require("components/pages/database/aiHub/aiAgents/AiAgents");
import EditAiAgent = require("components/pages/database/aiHub/aiAgents/edit/EditAiAgent");
import ChatAiAgent = require("components/pages/database/aiHub/aiAgents/chat/ChatAiAgent");
import AiTasksErrors = require("components/pages/database/aiHub/aiTasks/AiTasksErrors");
import footer = require("common/shell/footer");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getAiHubMenuItem;

function getAiHubMenuItem(appUrls: computedAppUrls) {
    const statsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/ai/connectionStrings',
            moduleId: reactUtils.bridgeToReact(AiConnectionStrings.default, "nonShardedView"),
            title: t("aiHub.connectionStrings"),
            nav: true,
            css: 'icon-manage-connection-strings ai-hub',
            dynamicHash: appUrls.aiConnectionStrings
        }),
        new leafMenuItem({
            route: 'databases/ai/agents',
            moduleId: reactUtils.bridgeToReact(AiAgents.default, "nonShardedView"),
            title: t("aiHub.agents"),
            nav: true,
            css: 'icon-ai-agents ai-hub',
            dynamicHash: appUrls.aiAgents
        }),
        new leafMenuItem({
            route: 'databases/ai/agents/edit',
            moduleId: reactUtils.bridgeToReact(EditAiAgent.default, "nonShardedView"),
            title: t("aiHub.agent.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editAiAgentUrl,
            itemRouteToHighlight: 'databases/ai/agents',
            search: {
                overrideTitle: t("aiHub.agent.addNew"),
                alternativeTitles: [t("aiHub.agent.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/ai/agents/chat',
            moduleId: reactUtils.bridgeToReact(ChatAiAgent.default, "nonShardedView"),
            title: t("aiHub.agentChat"),
            nav: false,
            css: "icon-llm",
            itemRouteToHighlight: 'databases/ai/agents',
            search: {
                isExcluded: true
            }
        }),
        new leafMenuItem({
            route: 'databases/ai/tasks',
            moduleId: reactUtils.bridgeToReact(AiTasks.default, "shardedView"),
            shardingMode: "allShards",
            title: t("aiHub.tasks"),
            nav: true,
            css: 'icon-manage-ongoing-tasks ai-hub',
            dynamicHash: appUrls.aiTasks
            
        }),
        new leafMenuItem({
            route: 'databases/ai/tasksStats',
            moduleId: require('viewmodels/database/aiHub/aiTasksStats'),
            shardingMode: "singleShard",
            title: t("aiHub.taskStats"),
            nav: true,
            css: 'icon-replication-stats ai-hub',
            dynamicHash: appUrls.aiTasksStats
        }),
        new leafMenuItem({
            route: 'databases/ai/tasksErrors',
            moduleId: reactUtils.bridgeToReact(AiTasksErrors.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("aiHub.taskErrors"),
            nav: true,
            css: 'icon-tasks-errors ai-hub',
            requiredAccess: "DatabaseReadWrite",
            dynamicHash: appUrls.aiTasksErrors,
            badgeData: ko.pureComputed(() => { return footer.default.stats() ? footer.default.stats().countOfAiTasksErrors() : null; })
        }),
    ];

    return new intermediateMenuItem(t("aiHub.title"), statsItems, "icon-ai");
}
