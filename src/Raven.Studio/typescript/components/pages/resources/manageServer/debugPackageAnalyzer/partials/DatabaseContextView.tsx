import React, { memo, useMemo } from "react";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import DatabaseStats from "./DatabaseStats";
import DatabaseOngoingTasks from "./DatabaseOngoingTasks";
import DatabaseIndexStats from "./DatabaseIndexStats";
import DatabaseIndexPerformanceLink from "./DatabaseIndexPerformanceLink";
import DatabaseIndexDefinitions from "./DatabaseIndexDefinitions";
import DatabaseIndexErrors from "./DatabaseIndexErrors";
import DatabaseSettings from "./DatabaseSettings";
import AnalysisSection from "./AnalysisSection";
import genUtils from "common/generalUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface DatabaseContextViewProps {
    summary: DebugPackageAnalysisSummary;
    database: string;
    selectedNode: string;
}

interface OverviewRow {
    node: string;
    online: boolean;
    disabled: boolean;
    documents: number;
    indexes: number;
    erroredIndexes: number;
    indexingErrors: number;
    ongoingTasks: number;
    alerts: number;
    performanceHints: number;
    lastBackup: string;
}

interface StorageRow {
    node: string;
    data: number;
    temp: number;
}

// Per-node view of a single database. Documents/indexes/storage are reported per node, so this
// surfaces divergence between nodes (replication lag, node-local indexing errors, uneven storage).
export default memo(function DatabaseContextView({ summary, database, selectedNode }: DatabaseContextViewProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const overviewRows = useMemo(() => collectOverviewRows(summary, database, t), [summary, database, t]);
    const storageRows = useMemo(() => collectStorageRows(summary, database), [summary, database]);
    const overviewNodeTags = useMemo(() => overviewRows.map((row) => row.node), [overviewRows]);

    const totalData = storageRows.reduce((sum, row) => sum + row.data, 0);
    const totalTemp = storageRows.reduce((sum, row) => sum + row.temp, 0);

    if (overviewRows.length === 0 && storageRows.length === 0) {
        return <EmptySet>{t("databaseContext.empty", { database })}</EmptySet>;
    }

    return (
        <div className="database-context vstack gap-3">
            <h2 className="database-context-group-heading">{t("databaseContext.groups.global")}</h2>

            <AnalysisSection
                id="database-overview"
                label={t("databaseContext.overview.title")}
                group={t("databaseContext.groups.global")}
            >
                <div className="panel-bg-1 rounded">
                    <div className="p-4 vstack gap-3">
                        <h3 className="mb-0">{t("databaseContext.overview.title")}</h3>
                        {overviewRows.length === 0 ? (
                            <EmptySet compact className="justify-content-center">
                                {t("databaseContext.overview.empty")}
                            </EmptySet>
                        ) : (
                            <div className="vstack gap-3">
                                {overviewRows.map((row) => (
                                    <div key={row.node}>
                                        <div className="hstack gap-1 align-items-center mb-1">
                                            <StudioTrans
                                                ns="debugPackageAnalyzer"
                                                i18nKey="databaseContext.nodeHeading"
                                                components={{ pill: <NodeTagPill tag={row.node} /> }}
                                            />
                                        </div>
                                        <div className="overview-stats gap-2">
                                            <StatTile
                                                label={t("databaseContext.overview.nodeStatus")}
                                                icon={row.disabled ? "cancel" : row.online ? "check" : "disconnected"}
                                                iconColor={row.disabled ? "warning" : row.online ? "success" : "danger"}
                                                value={
                                                    row.disabled
                                                        ? t("databaseContext.overview.nodeStatusDisabled")
                                                        : row.online
                                                          ? t("databaseContext.overview.nodeStatusOnline")
                                                          : t("databaseContext.overview.nodeStatusOffline")
                                                }
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.documents")}
                                                icon="documents"
                                                value={formatCount(row.documents)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.indexes")}
                                                icon="indexing"
                                                value={
                                                    row.erroredIndexes > 0 ? (
                                                        <>
                                                            {formatCount(row.indexes)}{" "}
                                                            <Badge bg="danger" pill>
                                                                <Icon icon="danger" margin="m-0" /> {row.erroredIndexes}
                                                            </Badge>
                                                        </>
                                                    ) : (
                                                        formatCount(row.indexes)
                                                    )
                                                }
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.indexingErrors")}
                                                icon="index-errors"
                                                iconColor={row.indexingErrors > 0 ? "danger" : undefined}
                                                valueColor={row.indexingErrors > 0 ? "danger" : undefined}
                                                value={formatCount(row.indexingErrors)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.ongoingTasks")}
                                                icon="ongoing-tasks"
                                                value={formatCount(row.ongoingTasks)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.alerts")}
                                                icon="alerts"
                                                iconColor={row.alerts > 0 ? "warning" : undefined}
                                                valueColor={row.alerts > 0 ? "warning" : undefined}
                                                value={formatCount(row.alerts)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.performanceHints")}
                                                icon="performance"
                                                value={formatCount(row.performanceHints)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.overview.lastBackup")}
                                                icon="backup"
                                                value={row.lastBackup}
                                            />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </AnalysisSection>

            <AnalysisSection
                id="database-storage"
                label={t("databaseContext.storage.title")}
                group={t("databaseContext.groups.global")}
            >
                <div className="panel-bg-1 rounded">
                    <div className="p-4 vstack gap-3">
                        <h3 className="mb-0">{t("databaseContext.storage.title")}</h3>
                        <div className="overview-stats gap-2">
                            <StatTile
                                label={t("databaseContext.storage.dataSize")}
                                icon="ravendb-data"
                                value={genUtils.formatBytesToSize(totalData)}
                            />
                            <StatTile
                                label={t("databaseContext.storage.tempSize")}
                                icon="clock"
                                value={genUtils.formatBytesToSize(totalTemp)}
                            />
                            <StatTile
                                label={t("databaseContext.storage.totalSize")}
                                icon="sum"
                                value={genUtils.formatBytesToSize(totalData + totalTemp)}
                            />
                        </div>
                        {storageRows.length === 0 ? (
                            <EmptySet compact className="justify-content-center">
                                {t("databaseContext.storage.empty")}
                            </EmptySet>
                        ) : (
                            <div className="vstack gap-3">
                                {storageRows.map((row) => (
                                    <div key={row.node}>
                                        <div className="hstack gap-1 align-items-center mb-1">
                                            <StudioTrans
                                                ns="debugPackageAnalyzer"
                                                i18nKey="databaseContext.nodeHeading"
                                                components={{ pill: <NodeTagPill tag={row.node} /> }}
                                            />
                                        </div>
                                        <div className="overview-stats gap-2">
                                            <StatTile
                                                label={t("databaseContext.storage.dataSize")}
                                                icon="ravendb-data"
                                                value={genUtils.formatBytesToSize(row.data)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.storage.tempSize")}
                                                icon="clock"
                                                value={genUtils.formatBytesToSize(row.temp)}
                                            />
                                            <StatTile
                                                label={t("databaseContext.storage.totalSize")}
                                                icon="sum"
                                                value={genUtils.formatBytesToSize(row.data + row.temp)}
                                            />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </AnalysisSection>

            <h2 className="database-context-group-heading">{t("databaseContext.groups.nodeScoped")}</h2>

            <AnalysisSection
                id="database-stats"
                label={t("databaseContext.sections.statistics")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseStats packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection
                id="database-index-stats"
                label={t("databaseContext.sections.indexStats")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseIndexStats packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection
                id="database-index-performance"
                label={t("databaseContext.sections.indexingPerformance")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseIndexPerformanceLink packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection
                id="database-index-definitions"
                label={t("databaseContext.sections.indexDefinitions")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseIndexDefinitions packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection
                id="database-index-errors"
                label={t("databaseContext.sections.indexErrors")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseIndexErrors packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection
                id="database-ongoing-tasks"
                label={t("databaseContext.sections.ongoingTasks")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseOngoingTasks
                    packageId={summary.PackageId}
                    database={database}
                    nodes={overviewNodeTags}
                    selectedNode={selectedNode}
                />
            </AnalysisSection>

            <AnalysisSection
                id="database-settings"
                label={t("databaseContext.sections.settings")}
                group={t("databaseContext.groups.nodeScoped")}
            >
                <DatabaseSettings packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>
        </div>
    );
});

function formatCount(value: number): string {
    if (value == null || value < 0) {
        return "-";
    }
    return value.toLocaleString();
}

function collectOverviewRows(
    summary: DebugPackageAnalysisSummary,
    database: string,
    t: StudioTranslate<"debugPackageAnalyzer">
): OverviewRow[] {
    const rows: OverviewRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.DatabasesOverview?.Items ?? []).forEach((item) => {
            if (item.Database !== database || item.Irrelevant) {
                return;
            }
            const lastBackup = item.BackupInfo?.LastBackup;
            rows.push({
                node: nodeTag,
                online: item.Online,
                disabled: item.Disabled,
                documents: item.DocumentsCount,
                indexes: item.IndexesCount,
                erroredIndexes: item.ErroredIndexesCount,
                indexingErrors: item.IndexingErrorsCount,
                ongoingTasks: item.OngoingTasksCount,
                alerts: item.AlertsCount,
                performanceHints: item.PerformanceHintsCount,
                lastBackup: lastBackup
                    ? genUtils.formatUtcDateAsLocal(lastBackup)
                    : t("databaseContext.overview.lastBackupNever"),
            });
        });
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}

function collectStorageRows(summary: DebugPackageAnalysisSummary, database: string): StorageRow[] {
    const rows: StorageRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.DatabaseStorageUsage?.Items ?? []).forEach((item) => {
            if (item.Database !== database) {
                return;
            }
            rows.push({ node: nodeTag, data: item.Size, temp: item.TempBuffersSize });
        });
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}
