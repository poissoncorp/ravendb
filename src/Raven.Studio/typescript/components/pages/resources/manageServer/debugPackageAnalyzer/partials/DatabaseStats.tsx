import React, { memo } from "react";
import Badge from "react-bootstrap/Badge";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";
import { formatNumber } from "./analyzerUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type DatabaseStatistics = Raven.Client.Documents.Operations.DatabaseStatistics;

interface DatabaseStatsProps {
    packageId: string;
    database: string;
    node: string;
}

// On-demand per-node database statistics from the analyzer databases/stats endpoint - the full
// DatabaseStatistics (counts, sizes, change vector, stale indexes) beyond the per-node overview.
export default memo(function DatabaseStats({ packageId, database, node }: DatabaseStatsProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();

    const stats = useAsync(async () => {
        if (!node) {
            return null as DatabaseStatistics | null;
        }
        return manageServerService.getDebugPackageDatabaseStats(packageId, node, database);
    }, [packageId, node, database]);

    const info = stats.result;

    return (
        <div className="database-stats">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">{t("databaseStats.title")}</h3>
                    {stats.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> {t("databaseStats.loading", { node })}
                        </div>
                    ) : stats.error ? (
                        <RichAlert variant="danger">{t("databaseStats.loadError", { node })}</RichAlert>
                    ) : !info ? (
                        <EmptySet compact className="justify-content-center">
                            {t("databaseStats.empty", { database, node })}
                        </EmptySet>
                    ) : (
                        <>
                            <div className="overview-stats gap-2">
                                <StatTile
                                    label={t("databaseStats.documents")}
                                    icon="documents"
                                    value={formatNumber(info.CountOfDocuments)}
                                />
                                <StatTile
                                    label={t("databaseStats.indexes")}
                                    icon="indexing"
                                    value={formatNumber(info.CountOfIndexes)}
                                />
                                <StatTile
                                    label={t("databaseStats.attachments")}
                                    icon="attachment"
                                    value={formatNumber(info.CountOfAttachments)}
                                />
                                <StatTile
                                    label={t("databaseStats.revisions")}
                                    icon="revisions"
                                    value={formatNumber(info.CountOfRevisionDocuments)}
                                />
                                <StatTile
                                    label={t("databaseStats.conflicts")}
                                    icon="conflicts"
                                    value={formatNumber(info.CountOfDocumentsConflicts)}
                                />
                                <StatTile
                                    label={t("databaseStats.tombstones")}
                                    icon="tombstones"
                                    value={formatNumber(info.CountOfTombstones)}
                                />
                                <StatTile
                                    label={t("databaseStats.counters")}
                                    icon="new-counter"
                                    value={formatNumber(info.CountOfCounterEntries)}
                                />
                                <StatTile
                                    label={t("databaseStats.timeSeriesSegments")}
                                    icon="timeseries-settings"
                                    value={formatNumber(info.CountOfTimeSeriesSegments)}
                                />
                                <StatTile
                                    label={t("databaseStats.sizeOnDisk")}
                                    icon="storage"
                                    value={info.SizeOnDisk?.HumaneSize ?? "-"}
                                />
                                <StatTile
                                    label={t("databaseStats.tempBuffers")}
                                    icon="clock"
                                    value={info.TempBuffersSizeOnDisk?.HumaneSize ?? "-"}
                                />
                                <StatTile label={t("databaseStats.databaseId")} icon="hash" value={info.DatabaseId} />
                                <StatTile
                                    label={t("databaseStats.lastIndexingTime")}
                                    icon="index-history"
                                    value={
                                        info.LastIndexingTime
                                            ? genUtils.formatUtcDateAsLocal(info.LastIndexingTime)
                                            : t("databaseStats.never")
                                    }
                                />
                                <StatTile
                                    label={t("databaseStats.architecture")}
                                    icon="processor"
                                    value={
                                        info.Is64Bit
                                            ? t("databaseStats.architecture64Bit")
                                            : t("databaseStats.architecture32Bit")
                                    }
                                />
                                <StatTile
                                    label={t("databaseStats.changeVector")}
                                    icon="vector"
                                    value={info.DatabaseChangeVector || "-"}
                                />
                            </div>

                            {info.StaleIndexes?.length > 0 && (
                                <div>
                                    <div className="small-label mb-1">
                                        {t("databaseStats.staleIndexes", { count: info.StaleIndexes.length })}
                                    </div>
                                    <div className="hstack gap-1 flex-wrap">
                                        {info.StaleIndexes.map((name) => (
                                            <Badge key={name} bg="warning" pill>
                                                {name}
                                            </Badge>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
});
