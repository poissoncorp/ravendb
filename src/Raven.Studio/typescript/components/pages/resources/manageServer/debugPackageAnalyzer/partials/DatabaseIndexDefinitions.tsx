import React, { memo, useCallback, useState } from "react";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import Badge from "react-bootstrap/Badge";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { Icon } from "components/common/Icon";
import Code, { CodeLanguage } from "components/common/Code";
import IndexUtils from "components/utils/IndexUtils";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";
import "./DatabaseIndexDefinitions.scss";

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;
type IndexFieldOptions = Raven.Client.Documents.Indexes.IndexFieldOptions;

interface DatabaseIndexDefinitionsProps {
    packageId: string;
    database: string;
    node: string;
}

// On-demand per-node index definitions from the analyzer databases/indexes endpoint. The map/reduce
// functions render through the shared Code component (read-only counterpart of the live index editor).
export default memo(function DatabaseIndexDefinitions({ packageId, database, node }: DatabaseIndexDefinitionsProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();
    // collapsed by default - map/reduce bodies can be large; expand selectively or all at once
    const [expanded, setExpanded] = useState<Set<string>>(new Set());

    const definitions = useAsync(async () => {
        if (!node) {
            return [] as IndexDefinition[];
        }
        return manageServerService.getDebugPackageDatabaseIndexDefinitions(packageId, node, database);
    }, [packageId, node, database]);

    const indexes = definitions.result ?? [];
    const allExpanded = indexes.length > 0 && indexes.every((index) => expanded.has(index.Name));

    const toggleIndex = useCallback(
        (name: string) =>
            setExpanded((prev) => {
                const next = new Set(prev);
                if (next.has(name)) {
                    next.delete(name);
                } else {
                    next.add(name);
                }
                return next;
            }),
        []
    );

    const toggleAll = () => setExpanded(allExpanded ? new Set() : new Set(indexes.map((index) => index.Name)));

    return (
        <div className="database-index-definitions">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                        <h3 className="m-0">{t("indexDefinitions.title")}</h3>
                        {indexes.length > 0 && (
                            <Button variant="link" size="sm" className="ms-auto" onClick={toggleAll}>
                                <Icon icon={allExpanded ? "collapse" : "expand"} />
                                {allExpanded ? t("indexDefinitions.collapseAll") : t("indexDefinitions.expandAll")}
                            </Button>
                        )}
                    </div>

                    {definitions.loading ? (
                        <div className="panel-bg-1 rounded">
                            <div className="p-4">
                                <div className="hstack gap-2 justify-content-center text-muted py-3">
                                    <Spinner size="sm" /> {t("indexDefinitions.loading", { node })}
                                </div>
                            </div>
                        </div>
                    ) : definitions.error ? (
                        <RichAlert variant="danger">{t("indexDefinitions.loadError", { node })}</RichAlert>
                    ) : indexes.length === 0 ? (
                        <div className="panel-bg-1 rounded">
                            <div className="p-4">
                                <EmptySet compact className="justify-content-center">
                                    {t("indexDefinitions.empty", { database, node })}
                                </EmptySet>
                            </div>
                        </div>
                    ) : (
                        <div className="vstack">
                            {indexes.map((index) => (
                                <IndexDefinitionPanel
                                    key={index.Name}
                                    index={index}
                                    expanded={expanded.has(index.Name)}
                                    onToggle={toggleIndex}
                                />
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
});

interface IndexDefinitionPanelProps {
    index: IndexDefinition;
    expanded: boolean;
    onToggle: (name: string) => void;
}

const IndexDefinitionPanel = memo(function IndexDefinitionPanel({
    index,
    expanded,
    onToggle,
}: IndexDefinitionPanelProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const language: CodeLanguage = index.Type?.startsWith("JavaScript") ? "javascript" : "csharp";
    const maps = index.Maps ?? [];
    const fields = Object.entries(index.Fields ?? {});
    const sources = Object.keys(index.AdditionalSources ?? {});
    const configuration = Object.entries(index.Configuration ?? {});
    const extraDetails = collectExtraDetails(index, t);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>{index.Name}</RichPanelName>
                </RichPanelInfo>
                <div className="hstack gap-3 flex-wrap align-items-center ms-auto">
                    <span className="hstack gap-1" title={indexTypeDescription(index.Type, t)}>
                        <Icon icon={IndexUtils.indexTypeIcon(index.Type) ?? "index"} margin="m-0" />
                        {IndexUtils.formatType(index.Type)}
                    </span>
                    <span className="hstack gap-1" title={sourceTypeDescription(index.SourceType, t)}>
                        <Icon icon={sourceTypeIcon(index.SourceType)} margin="m-0" />
                        {sourceTypeLabel(index.SourceType, t)}
                    </span>
                    {index.State && index.State !== "Normal" && (
                        <Badge bg="warning" pill>
                            {index.State}
                        </Badge>
                    )}
                    {index.OutputReduceToCollection && (
                        <Badge bg="success" pill>
                            <Icon icon="documents" margin="m-0" />{" "}
                            {t("indexDefinitions.outputTo", { collection: index.OutputReduceToCollection })}
                        </Badge>
                    )}
                    <span className="hstack gap-1" title={t("indexDefinitions.priorityTitle")}>
                        <Icon icon={priorityIcon(index.Priority)} margin="m-0" />
                        {priorityLabel(index.Priority, t)}
                    </span>
                    <span className="hstack gap-1" title={t("indexDefinitions.lockModeTitle")}>
                        <Icon icon={lockIcon(index.LockMode)} margin="m-0" />
                        {lockLabel(index.LockMode, t)}
                    </span>
                </div>
                <RichPanelActions>
                    <Button
                        variant="secondary"
                        active={expanded}
                        onClick={() => onToggle(index.Name)}
                        title={expanded ? t("indexDefinitions.collapse") : t("indexDefinitions.expand")}
                        className="btn-toggle-panel"
                    >
                        <Icon icon={expanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={expanded} unmountOnExit>
                <div className="index-definition-body">
                    {extraDetails.length > 0 && (
                        <RichPanelDetails>
                            {extraDetails.map((detail) => (
                                <RichPanelDetailItem key={detail.label} label={detail.label}>
                                    {detail.value}
                                </RichPanelDetailItem>
                            ))}
                        </RichPanelDetails>
                    )}
                    <div className="p-4 vstack gap-3">
                        {maps.map((map, i) => (
                            <div key={i}>
                                <div className="small-label mb-1">
                                    {t("indexDefinitions.map", {
                                        context: maps.length > 1 ? "numbered" : "single",
                                        number: i + 1,
                                    })}
                                </div>
                                <Code language={language} code={map} />
                            </div>
                        ))}
                        {index.Reduce && (
                            <div>
                                <div className="small-label mb-1">{t("indexDefinitions.reduce")}</div>
                                <Code language={language} code={index.Reduce} />
                            </div>
                        )}
                        {fields.length > 0 && <FieldsTable fields={index.Fields} />}
                        {sources.length > 0 && (
                            <div>
                                <div className="small-label mb-1">{t("indexDefinitions.additionalSources")}</div>
                                <div className="hstack gap-1 flex-wrap">
                                    {sources.map((name) => (
                                        <Badge key={name} bg="secondary" pill>
                                            {name}
                                        </Badge>
                                    ))}
                                </div>
                            </div>
                        )}
                        {configuration.length > 0 && (
                            <div>
                                <div className="small-label mb-1">{t("indexDefinitions.configuration")}</div>
                                <div className="vstack">
                                    {configuration.map(([key, value]) => (
                                        <div key={key} className="small">
                                            <span>{key}</span>: {String(value)}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </Collapse>
        </RichPanel>
    );
});

function indexTypeDescription(type: IndexDefinition["Type"], t: StudioTranslate<"debugPackageAnalyzer">): string {
    switch (type) {
        case "Map":
            return t("indexDefinitions.indexType.map");
        case "MapReduce":
            return t("indexDefinitions.indexType.mapReduce");
        case "AutoMap":
            return t("indexDefinitions.indexType.autoMap");
        case "AutoMapReduce":
            return t("indexDefinitions.indexType.autoMapReduce");
        case "JavaScriptMap":
            return t("indexDefinitions.indexType.javaScriptMap");
        case "JavaScriptMapReduce":
            return t("indexDefinitions.indexType.javaScriptMapReduce");
        case "Faulty":
            return t("indexDefinitions.indexType.faulty");
        default:
            return null;
    }
}

function priorityIcon(priority: IndexDefinition["Priority"]): IconName {
    switch (priority) {
        case "Low":
            return "coffee";
        case "High":
            return "force";
        default:
            return "check";
    }
}

function priorityLabel(priority: IndexDefinition["Priority"], t: StudioTranslate<"debugPackageAnalyzer">): string {
    switch (priority ?? "Normal") {
        case "Low":
            return t("indexDefinitions.priority.low");
        case "Normal":
            return t("indexDefinitions.priority.normal");
        case "High":
            return t("indexDefinitions.priority.high");
        default:
            return priority;
    }
}

function lockIcon(lockMode: IndexDefinition["LockMode"]): IconName {
    switch (lockMode) {
        case "LockedIgnore":
            return "lock";
        case "LockedError":
            return "lock-error";
        default:
            return "unlock";
    }
}

function lockLabel(lockMode: IndexDefinition["LockMode"], t: StudioTranslate<"debugPackageAnalyzer">): string {
    switch (lockMode) {
        case "LockedIgnore":
            return t("indexDefinitions.lockMode.lockedIgnore");
        case "LockedError":
            return t("indexDefinitions.lockMode.lockedError");
        default:
            return t("indexDefinitions.lockMode.unlocked");
    }
}

function sourceTypeIcon(sourceType: IndexDefinition["SourceType"]): IconName {
    switch (sourceType) {
        case "TimeSeries":
            return "timeseries";
        case "Counters":
            return "new-counter";
        default:
            return "documents";
    }
}

function sourceTypeLabel(
    sourceType: IndexDefinition["SourceType"],
    t: StudioTranslate<"debugPackageAnalyzer">
): string {
    switch (sourceType) {
        case "Documents":
            return t("indexDefinitions.sourceType.documents");
        case "TimeSeries":
            return t("indexDefinitions.sourceType.timeSeries");
        case "Counters":
            return t("indexDefinitions.sourceType.counters");
        default:
            return sourceType;
    }
}

function sourceTypeDescription(
    sourceType: IndexDefinition["SourceType"],
    t: StudioTranslate<"debugPackageAnalyzer">
): string {
    switch (sourceType) {
        case "Documents":
            return t("indexDefinitions.sourceType.documentsTitle");
        case "TimeSeries":
            return t("indexDefinitions.sourceType.timeSeriesTitle");
        case "Counters":
            return t("indexDefinitions.sourceType.countersTitle");
        default:
            return null;
    }
}

function FieldsTable({ fields }: { fields: { [key: string]: IndexFieldOptions } }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const entries = Object.entries(fields);

    return (
        <div>
            <div className="small-label mb-1">{t("indexDefinitions.fields.title", { count: entries.length })}</div>
            <Table responsive className="m-0 align-middle">
                <thead>
                    <tr>
                        <th>{t("indexDefinitions.fields.field")}</th>
                        <th>{t("indexDefinitions.fields.indexing")}</th>
                        <th>{t("indexDefinitions.fields.storage")}</th>
                        <th>{t("indexDefinitions.fields.analyzer")}</th>
                        <th>{t("indexDefinitions.fields.suggestions")}</th>
                        <th>{t("indexDefinitions.fields.termVector")}</th>
                    </tr>
                </thead>
                <tbody>
                    {entries.map(([name, options]) => (
                        <tr key={name}>
                            <td>{name}</td>
                            <td>{options.Indexing ?? "-"}</td>
                            <td>{options.Storage ?? "-"}</td>
                            <td>{options.Analyzer ?? "-"}</td>
                            <td>{options.Suggestions ? t("yes") : "-"}</td>
                            <td>{options.TermVector ?? "-"}</td>
                        </tr>
                    ))}
                </tbody>
            </Table>
        </div>
    );
}

// Map-reduce-output, deployment and assembly properties that the live editor surfaces but that the
// summary/stats views omit - shown as detail chips only when the index actually sets them.
function collectExtraDetails(
    index: IndexDefinition,
    t: StudioTranslate<"debugPackageAnalyzer">
): { label: string; value: string }[] {
    const details: { label: string; value: string }[] = [];

    if (index.PatternReferencesCollectionName) {
        details.push({
            label: t("indexDefinitions.details.referencesCollection"),
            value: index.PatternReferencesCollectionName,
        });
    }
    if (index.PatternForOutputReduceToCollectionReferences) {
        details.push({
            label: t("indexDefinitions.details.outputReferencePattern"),
            value: index.PatternForOutputReduceToCollectionReferences,
        });
    }
    if (index.ReduceOutputIndex != null) {
        details.push({
            label: t("indexDefinitions.details.reduceOutputIndex"),
            value: String(index.ReduceOutputIndex),
        });
    }
    if (index.DeploymentMode) {
        details.push({ label: t("indexDefinitions.details.deploymentMode"), value: index.DeploymentMode });
    }
    if (index.ArchivedDataProcessingBehavior) {
        details.push({
            label: t("indexDefinitions.details.archivedData"),
            value: index.ArchivedDataProcessingBehavior,
        });
    }
    if (index.CompoundFields?.length) {
        details.push({
            label: t("indexDefinitions.details.compoundFields"),
            value: index.CompoundFields.map((group) => group.join(" + ")).join(", "),
        });
    }
    if (index.AdditionalAssemblies?.length) {
        details.push({
            label: t("indexDefinitions.details.additionalAssemblies"),
            value: String(index.AdditionalAssemblies.length),
        });
    }

    return details;
}
