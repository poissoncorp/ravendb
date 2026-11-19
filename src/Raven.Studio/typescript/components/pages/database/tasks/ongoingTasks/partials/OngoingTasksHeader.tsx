import { RefObject, useEffect, useRef } from "react";
import Button from "react-bootstrap/Button";
import { useResizeObserver } from "hooks/useResizeObserver";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { StickyHeader } from "components/common/StickyHeader";
import { useRavenLink } from "hooks/useRavenLink";
import OngoingTaskSelectActions from "./OngoingTaskSelectActions";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { OngoingTasksState } from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksReducer";
import appUrl from "common/appUrl";
import OngoingTasksFilter, {
    OngoingTaskFilterType,
    OngoingTasksFilterCriteria,
} from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksFilter";
import { InputItem } from "components/models/common";
import { exhaustiveStringTuple } from "components/utils/common";
import assertUnreachable from "components/utils/assertUnreachable";
import { useOngoingTasksOperations } from "components/pages/database/tasks/shared/shared";
import OngoingTaskOperationConfirm from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import { useAppUrls } from "hooks/useAppUrls";
import AiTasksInfoHub from "components/pages/database/aiHub/aiTasks/AiTasksInfoHub";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface OngoingTasksHeaderProps {
    tasks: OngoingTasksState;
    hasInternalReplication: boolean;
    allTasksCount: number;
    selectedTaskIds: number[];
    filter: OngoingTasksFilterCriteria;
    setFilter: (x: OngoingTasksFilterCriteria) => void;
    reload: () => void;
    filteredDatabaseTaskIds: number[];
    setSelectedTaskIds: (tasks: number[]) => void;
    isAiOnly?: boolean;
}

function useStickyHeaderHeightVariable(stickyHeaderRef: RefObject<HTMLDivElement>) {
    const { height } = useResizeObserver({ ref: stickyHeaderRef, box: "border-box" });

    useEffect(() => {
        const page = stickyHeaderRef.current?.parentElement;
        if (!page || height == null) {
            return;
        }

        page.style.setProperty("--sticky-header-height", `${height}px`);
        return () => {
            page.style.removeProperty("--sticky-header-height");
        };
    }, [height]);
}

export function OngoingTasksHeader(props: OngoingTasksHeaderProps) {
    const t = useStudioTranslation("ongoingTasks");
    const { forCurrentDatabase } = useAppUrls();
    const {
        tasks,
        allTasksCount,
        selectedTaskIds,
        setFilter,
        filter,
        hasInternalReplication,
        filteredDatabaseTaskIds,
        reload,
        setSelectedTaskIds,
        isAiOnly,
    } = props;

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const { onTaskOperation, isTogglingStateAny, isDeletingAny, operationConfirm, cancelOperationConfirm } =
        useOngoingTasksOperations(reload);

    const getSelectedTaskShardedInfos = () =>
        [...tasks.tasks, ...tasks.subscriptions, ...tasks.replicationHubs]
            .filter((x) => selectedTaskIds.includes(x.shared.taskId))
            .map((x) => x.shared);

    const stickyHeaderRef = useRef<HTMLDivElement>(null);
    useStickyHeaderHeightVariable(stickyHeaderRef);

    return (
        <>
            <StickyHeader ref={stickyHeaderRef}>
                <div className="hstack gap-3 flex-wrap">
                    {hasDatabaseWriteAccess && (
                        <>
                            <div id="NewTaskButton">
                                <Button
                                    href={forCurrentDatabase.addNewOngoingTaskUrl(isAiOnly)()}
                                    className="rounded-pill"
                                >
                                    <Icon icon="ongoing-tasks" addon="plus" />
                                    {t("header.addTask", { context: isAiOnly ? "ai" : "database" })}
                                </Button>
                            </div>
                        </>
                    )}

                    <FlexGrow />

                    {isClusterAdminOrClusterNode && !isAiOnly && (
                        <a
                            className="btn btn-link btn-sm"
                            href={serverWideTasksUrl}
                            title={t("header.serverWideTasksTitle")}
                        >
                            <Icon icon="server-wide-tasks" />
                            {t("serverWideTasks")}
                        </a>
                    )}

                    {isAiOnly ? <AiTasksInfoHub /> : <AboutView />}
                </div>

                {allTasksCount > 0 && (
                    <div className="mt-3">
                        <OngoingTasksFilter
                            filter={filter}
                            setFilter={setFilter}
                            filterByStatusOptions={getFilterByStatusOptions(t, tasks, hasInternalReplication)}
                            tasksCount={allTasksCount}
                            isAiOnly={isAiOnly}
                        />
                    </div>
                )}
                {allTasksCount > 0 && hasDatabaseAdminAccess && (
                    <OngoingTaskSelectActions
                        allTasks={filteredDatabaseTaskIds}
                        selectedTasks={selectedTaskIds}
                        setSelectedTasks={setSelectedTaskIds}
                        onTaskOperation={(type) => onTaskOperation(type, getSelectedTaskShardedInfos())}
                        isTogglingState={isTogglingStateAny}
                        isDeleting={isDeletingAny}
                    />
                )}
            </StickyHeader>
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
        </>
    );
}

function AboutView() {
    const t = useStudioTranslation("ongoingTasks");
    const tCommon = useStudioTranslation("common");
    const ongoingTasksDocsLink = useRavenLink({ hash: "K4ZTNA" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>
                            <StudioTrans
                                ns="ongoingTasks"
                                i18nKey="about.intro"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="ongoingTasks"
                                i18nKey="about.thisView"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="ongoingTasks"
                                i18nKey="about.background"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                            <ul className="margin-top-xxs">
                                <li>{t("about.responsibleNode")}</li>
                                <li className="margin-top-xxs">{t("about.clusterDecides")}</li>
                            </ul>
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{tCommon("usefulLinks")}</div>
                <a href={ongoingTasksDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}

function getFilterByStatusOptions(
    t: StudioTranslate<"ongoingTasks">,
    state: OngoingTasksState,
    hasInternalReplication: boolean
): InputItem<OngoingTaskFilterType>[] {
    const backupCount = state.tasks.filter((x) => x.shared.taskType === "Backup").length;
    const subscriptionCount = state.subscriptions.length;

    const etlCount = state.tasks.filter((x) => x.shared.taskType.endsWith("Etl")).length;

    const sinkCount = state.tasks.filter(
        (x) => x.shared.taskType === "KafkaQueueSink" || x.shared.taskType === "RabbitQueueSink"
    ).length;

    const aiCount = state.tasks.filter((x) => ["GenAi", "EmbeddingsGeneration"].includes(x.shared.taskType)).length;
    const internalReplicationCount = hasInternalReplication ? 1 : 0;
    const replicationHubCount = state.replicationHubs.length;
    const replicationSinkCount = state.tasks.filter((x) => x.shared.taskType === "PullReplicationAsSink").length;
    const externalReplicationCount = state.tasks.filter((x) => x.shared.taskType === "Replication").length;
    const replicationCount =
        externalReplicationCount + replicationHubCount + replicationSinkCount + internalReplicationCount;

    return exhaustiveStringTuple<OngoingTaskFilterType>()(
        "AI",
        "Replication",
        "ETL",
        "Sink",
        "Backup",
        "Subscription"
    ).map((filterType) => {
        switch (filterType) {
            case "AI":
                return {
                    label: t("filterType.ai"),
                    value: filterType,
                    count: aiCount,
                };
            case "Replication":
                return {
                    label: t("filterType.replication"),
                    value: filterType,
                    count: replicationCount,
                };
            case "ETL":
                return { label: t("filterType.etl"), value: filterType, count: etlCount };
            case "Sink":
                return { label: t("filterType.sink"), value: filterType, count: sinkCount };
            case "Backup":
                return { label: t("filterType.backup"), value: filterType, count: backupCount };
            case "Subscription":
                return { label: t("filterType.subscription"), value: filterType, count: subscriptionCount };
            default:
                assertUnreachable(filterType);
        }
    });
}
