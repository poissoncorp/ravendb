import React, { ChangeEvent, useMemo } from "react";
import { Icon } from "components/common/Icon";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Select, { SelectOption } from "components/common/select/Select";
import { InputItem } from "components/models/common";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { EtlHealthStatus, GroupByType, TasksFiltersState } from "../utils/tasksErrorsUtils";
import { useUrlFilters } from "components/hooks/useUrlFilters";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

export type { TasksFiltersState };

export function getTaskHealthOptions(t: StudioTranslate<"tasksErrors">): SelectOption<EtlHealthStatus>[] {
    return [
        { label: t("health.healthy"), value: "Healthy" },
        { label: t("health.failed"), value: "Failed" },
        { label: t("health.impaired"), value: "Impaired" },
    ];
}

export function getTaskTypeOptions(t: StudioTranslate<"tasksErrors">): SelectOption<StudioTaskType>[] {
    return [
        { label: "RavenDB ETL", value: "RavenEtl" },
        { label: "SQL ETL", value: "SqlEtl" },
        { label: "Snowflake ETL", value: "SnowflakeEtl" },
        { label: t("filters.taskTypes.azureQueueStorageEtl"), value: "AzureQueueStorageQueueEtl" },
        { label: "OLAP ETL", value: "OlapEtl" },
        { label: "Kafka ETL", value: "KafkaQueueEtl" },
        { label: t("filters.taskTypes.elasticSearchEtl"), value: "ElasticSearchEtl" },
        { label: "RabbitMQ ETL", value: "RabbitQueueEtl" },
        { label: t("filters.taskTypes.amazonSqsEtl"), value: "AmazonSqsQueueEtl" },
        { label: t("filters.taskTypes.embeddingsGeneration"), value: "EmbeddingsGeneration" },
        { label: t("filters.taskTypes.genAi"), value: "GenAi" },
        { label: t("filters.taskTypes.cdcSink"), value: "CdcSink" },
    ];
}

export function getGroupByOptions(t: StudioTranslate<"tasksErrors">): InputItem<GroupByType>[] {
    return [
        { value: "task", label: t("filters.groupByTask"), badgeColor: "secondary" },
        { value: "none", label: t("filters.groupByNone"), badgeColor: "secondary" },
    ];
}

export function useTasksFilters(
    buildUrl: (filters: TasksFiltersState) => string,
    initialSearchText = "",
    initialTaskTypes: StudioTaskType[] = [],
    initialNodeTags: string[] = [],
    initialShardNumbers: string[] = [],
    initialHealthStatuses: EtlHealthStatus[] = [],
    extraDeps: unknown[] = []
): [TasksFiltersState, (patch: Partial<TasksFiltersState>) => void] {
    return useUrlFilters<TasksFiltersState>(
        {
            searchText: initialSearchText,
            nodeTags: initialNodeTags,
            shardNumbers: initialShardNumbers,
            healthStatuses: initialHealthStatuses,
            taskTypes: initialTaskTypes,
        },
        buildUrl,
        extraDeps
    );
}

interface TasksFiltersProps {
    selectedGroupByType: GroupByType;
    setSelectedGroupByType: (x: GroupByType) => void;
    filters: TasksFiltersState;
    updateFilters: (patch: Partial<TasksFiltersState>) => void;
}

export function TasksFilters({
    setSelectedGroupByType,
    selectedGroupByType,
    filters,
    updateFilters,
}: TasksFiltersProps) {
    const t = useStudioTranslation("tasksErrors");
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const taskHealthOptions = useMemo(() => getTaskHealthOptions(t), [t]);
    const taskTypeOptions = useMemo(() => getTaskTypeOptions(t), [t]);
    const groupByOptions = useMemo(() => getGroupByOptions(t), [t]);

    const nodeOptions = useMemo(() => {
        const tags: string[] = db.isSharded
            ? _.uniq(DatabaseUtils.getLocations(db).map((l) => l.nodeTag)).sort()
            : db.nodes.map((node) => node.tag);

        return tags.map((tag) => ({ label: t("filters.nodeOption", { tag }), value: tag }));
    }, [db, t]);

    const shardOptions = useMemo(() => {
        if (!db.isSharded) {
            return [];
        }
        const shardNumbers: number[] = _.uniq(
            DatabaseUtils.getLocations(db)
                .map((l) => l.shardNumber)
                .filter((n) => n != null)
        );

        return shardNumbers.map((n) => ({ label: t("filters.shardOption", { number: n }), value: String(n) }));
    }, [db, t]);

    return (
        <div className="hstack flex-wrap align-items-end gap-2 my-3 justify-content-start">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">{t("filters.searchLabel")}</div>
                <div className="clearable-input">
                    <Form.Control
                        type="text"
                        accessKey="/"
                        placeholder={t("filters.searchPlaceholder")}
                        title={t("filters.searchTitle")}
                        className="filtering-input"
                        value={filters.searchText}
                        onChange={(e: ChangeEvent<HTMLInputElement>) => updateFilters({ searchText: e.target.value })}
                    />
                    {filters.searchText && (
                        <div className="clear-button">
                            <Button variant="secondary" size="sm" onClick={() => updateFilters({ searchText: "" })}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div className="TaskErrorFilter">
                <div className="small-label ms-1 mb-1">{t("filters.nodeLabel")}</div>
                <Select
                    isMulti
                    isClearable
                    options={nodeOptions}
                    value={nodeOptions.filter((o) => filters.nodeTags.includes(o.value))}
                    onChange={(options) => updateFilters({ nodeTags: options ? options.map((o) => o.value) : [] })}
                />
            </div>
            {db.isSharded && (
                <div className="TaskErrorFilter">
                    <div className="small-label ms-1 mb-1">{t("filters.shardLabel")}</div>
                    <Select
                        isMulti
                        isClearable
                        options={shardOptions}
                        value={shardOptions.filter((o) => filters.shardNumbers.includes(o.value))}
                        onChange={(options) =>
                            updateFilters({ shardNumbers: options ? options.map((o) => o.value) : [] })
                        }
                    />
                </div>
            )}
            <div className="TaskErrorFilter">
                <div className="small-label ms-1 mb-1">{t("filters.taskTypeLabel")}</div>
                <Select
                    isMulti
                    isClearable
                    options={taskTypeOptions}
                    value={taskTypeOptions.filter((o) => filters.taskTypes.includes(o.value))}
                    onChange={(options) =>
                        updateFilters({
                            taskTypes: options ? options.map((o) => o.value) : [],
                        })
                    }
                />
            </div>
            <div className="TaskErrorFilter">
                <div className="small-label ms-1 mb-1">{t("filters.healthLabel")}</div>
                <Select
                    isMulti
                    isClearable
                    options={taskHealthOptions}
                    value={taskHealthOptions.filter((o) => filters.healthStatuses.includes(o.value))}
                    onChange={(options) =>
                        updateFilters({
                            healthStatuses: options ? options.map((o) => o.value) : [],
                        })
                    }
                />
            </div>
            <div>
                <div className="small-label ms-1 mb-1">{t("filters.groupByLabel")}</div>
                <MultiRadioToggle<GroupByType>
                    inputItems={groupByOptions}
                    selectedItem={selectedGroupByType}
                    setSelectedItem={(x) => setSelectedGroupByType(x)}
                />
            </div>
        </div>
    );
}
