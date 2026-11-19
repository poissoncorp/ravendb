import React, { memo, useMemo } from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "components/common/Icon";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import { formatNumber } from "./analyzerUtils";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type IndexStats = Raven.Client.Documents.Indexes.IndexStats;
type IndexState = Raven.Client.Documents.Indexes.IndexState;

const EMPTY_INDEX_STATS: IndexStats[] = [];

interface DatabaseIndexStatsProps {
    packageId: string;
    database: string;
    node: string;
}

interface DatabaseIndexStatsWithSizeProps extends DatabaseIndexStatsProps {
    width: number;
}

function useIndexStatsColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );

    const indexStatsColumns: ColumnDef<IndexStats>[] = useMemo(() => {
        const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
        return [
            {
                header: t("indexStats.columns.index"),
                accessorKey: "Name",
                cell: IndexNameCell,
                size: getSize(28),
            },
            {
                header: t("indexStats.columns.state"),
                accessorKey: "State",
                cell: ({ getValue }) => StateCell(getValue<IndexState>(), t),
                size: getSize(10),
            },
            {
                header: t("indexStats.columns.priority"),
                accessorKey: "Priority",
                size: getSize(10),
            },
            {
                header: t("indexStats.columns.type"),
                accessorKey: "Type",
                size: getSize(13),
            },
            {
                header: t("indexStats.columns.entries"),
                accessorKey: "EntriesCount",
                cell: ({ getValue }) => formatNumber(getValue<number>()),
                size: getSize(9),
            },
            {
                header: t("indexStats.columns.errors"),
                accessorKey: "ErrorsCount",
                cell: IndexErrorsCell,
                size: getSize(9),
            },
            {
                header: t("indexStats.columns.stale"),
                id: "stale",
                accessorFn: (index) => index.IsStale,
                cell: IndexStaleCell,
                size: getSize(10),
            },
            {
                header: t("indexStats.columns.lockMode"),
                accessorKey: "LockMode",
                size: getSize(11),
            },
        ];
    }, [bodyWidth, t]);

    return { indexStatsColumns };
}

// On-demand per-node index stats for the selected database, from the analyzer
// databases/indexes/stats endpoint (the summary only has aggregate indexing speed).
export default memo(function DatabaseIndexStats({ packageId, database, node }: DatabaseIndexStatsProps) {
    return (
        <SizeGetter
            render={({ width }) => (
                <DatabaseIndexStatsWithSize packageId={packageId} database={database} node={node} width={width} />
            )}
        />
    );
});

function DatabaseIndexStatsWithSize({ packageId, database, node, width }: DatabaseIndexStatsWithSizeProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();

    const stats = useAsync(async () => {
        if (!node) {
            return [] as IndexStats[];
        }
        return manageServerService.getDebugPackageDatabaseIndexStats(packageId, node, database);
    }, [packageId, node, database]);

    const indexes = stats.result ?? EMPTY_INDEX_STATS;

    const { indexStatsColumns } = useIndexStatsColumns(width);

    const table = useReactTable({
        data: indexes,
        columns: indexStatsColumns,
        enableSorting: indexes.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: indexes.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => row.Name,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(indexes.length, 400);

    return (
        <div className="database-index-stats">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">{t("indexStats.title")}</h3>
                    {stats.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> {t("indexStats.loading", { node })}
                        </div>
                    ) : stats.error ? (
                        <RichAlert variant="danger">{t("indexStats.loadError", { node })}</RichAlert>
                    ) : indexes.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            {t("indexStats.empty", { database, node })}
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function IndexNameCell({ getValue }: { getValue: () => unknown }) {
    const name = getValue() as string;
    return (
        <div className="text-truncate" title={name}>
            {name}
        </div>
    );
}

function IndexErrorsCell({ row }: { row: { original: IndexStats } }) {
    const count = row.original.ErrorsCount ?? 0;
    return <span className={count > 0 ? "text-danger" : ""}>{formatNumber(count)}</span>;
}

function IndexStaleCell({ getValue }: { getValue: () => unknown }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    return getValue() ? (
        <span className="hstack gap-1 text-warning">
            <Icon icon="warning" margin="m-0" /> {t("indexStats.stale")}
        </span>
    ) : (
        t("indexStats.upToDate")
    );
}

function StateCell(state: IndexState, t: StudioTranslate<"debugPackageAnalyzer">) {
    switch (state) {
        case "Error":
            return (
                <span className="hstack gap-1 text-danger">
                    <Icon icon="danger" margin="m-0" /> {t("indexStats.states.error")}
                </span>
            );
        case "Disabled":
            return (
                <span className="hstack gap-1 text-warning">
                    <Icon icon="cancel" margin="m-0" /> {t("indexStats.states.disabled")}
                </span>
            );
        case "Idle":
            return (
                <span className="hstack gap-1 text-muted">
                    <Icon icon="clock" margin="m-0" /> {t("indexStats.states.idle")}
                </span>
            );
        default:
            return (
                <span className="hstack gap-1 text-success">
                    <Icon icon="check" margin="m-0" /> {t("indexStats.states.normal")}
                </span>
            );
    }
}
