import { useMemo } from "react";
import { Getter, ColumnDef } from "@tanstack/react-table";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";
import SubscriptionInfoExtended = Raven.Server.Documents.TombstoneCleaner.TombstonesState.SubscriptionInfoExtended;

export function useTombstonesStateColumns(availableWidth: number) {
    const t = useStudioTranslation("tombstones");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const collectionsColumns: ColumnDef<TombstoneItem>[] = useMemo(
        () => [
            {
                header: t("perCollection.collection"),
                accessorKey: "Collection",
                cell: CellValueWrapper,
                size: getSize(28),
            },
            {
                header: t("perCollection.documentTask"),
                accessorFn: (x) => x.Documents.Component,
                cell: CellValueWrapper,
                size: getSize(16),
            },
            {
                header: t("perCollection.documentEtag"),
                accessorFn: (x) => x.Documents.Etag,
                cell: CellEtagWrapper,
                size: getSize(8),
            },
            {
                header: t("perCollection.timeSeriesTask"),
                accessorFn: (x) => x.TimeSeries.Component,
                cell: CellValueWrapper,
                size: getSize(16),
            },
            {
                header: t("perCollection.timeSeriesEtag"),
                accessorFn: (x) => x.TimeSeries.Etag,
                cell: CellEtagWrapper,
                size: getSize(8),
            },
            {
                header: t("perCollection.counterTask"),
                accessorFn: (x) => x.Counters.Component,
                cell: CellValueWrapper,
                size: getSize(16),
            },
            {
                header: t("perCollection.counterEtag"),
                accessorFn: (x) => x.Counters.Etag,
                cell: CellEtagWrapper,
                size: getSize(8),
            },
        ],
        [getSize, t]
    );

    const subscriptionsColumns: ColumnDef<SubscriptionInfoExtended>[] = useMemo(
        () => [
            {
                header: t("perTask.process"),
                accessorKey: "Process",
                cell: CellValueWrapper,
                size: getSize(15),
            },
            {
                header: t("perTask.name"),
                accessorKey: "Identifier",
                cell: CellValueWrapper,
                size: getSize(15),
            },
            {
                header: t("perTask.numberOfTombstonesLeft"),
                accessorKey: "NumberOfTombstoneLeft",
                cell: CellValueWrapper,
                size: getSize(10),
            },
            {
                header: t("perTask.tombstoneTypes"),
                accessorFn: (x) => formatTombstoneTypes(x.Types, x.Process, t),
                cell: CellValueWrapper,
                size: getSize(25),
            },
            {
                header: t("perTask.collection"),
                accessorKey: "Collection",
                cell: CellValueWrapper,
                size: getSize(10),
            },
            {
                header: t("perTask.etag"),
                accessorKey: "Etag",
                cell: CellEtagWrapper,
                size: getSize(10),
            },
            {
                header: t("perTask.cleanupStatus"),
                id: "CleanupStatus",
                accessorFn: (x) => (x.NumberOfTombstoneLeft > 0 ? t("perTask.blocking") : t("perTask.notBlocking")),
                cell: CellValueWrapper,
                size: getSize(15),
            },
        ],
        [getSize, t]
    );

    return {
        collectionsColumns,
        subscriptionsColumns,
        formatEtag: (value: number) => formatEtag(value, t),
    };
}

const etagMaxValue = 9223372036854776000; // in general Long.MAX_Value is 9223372036854775807 but we loose precision here

function formatEtag(value: number, t: StudioTranslate<"tombstones">) {
    if (value === etagMaxValue) {
        return t("etag.maxValue");
    }

    return value;
}
function formatTombstoneTypes(
    types: Raven.Server.Documents.TombstoneCleaner.TombstonesState.TombstoneTypes,
    process: Raven.Server.Documents.ITombstoneAware.TombstoneDeletionBlockerType,
    t: StudioTranslate<"tombstones">
) {
    if (!types) {
        return "";
    }

    if (process === "Index") {
        return t("perTask.tombstoneTypesDocuments", { documents: types.Documents });
    }

    return t("perTask.tombstoneTypesAll", {
        documents: types.Documents,
        timeSeries: types.TimeSeries,
        counters: types.Counters,
    });
}

function getEtagTitle(etagValue: number, t: StudioTranslate<"tombstones">) {
    if (etagValue === 0) {
        return t("etag.noneRemovableTitle");
    }

    if (etagValue < etagMaxValue) {
        return t("etag.removableUpToTitle", { etag: etagValue });
    }

    return t("etag.anyRemovableTitle");
}

function CellEtagWrapper({ getValue }: { getValue: Getter<number> }) {
    const t = useStudioTranslation("tombstones");
    const value = getValue();
    return <CellValue value={formatEtag(value, t)} title={getEtagTitle(value, t)} />;
}

function CellValue({ value, title }: { value: unknown; title?: string }) {
    return (
        <span title={title} className={`value-${typeof value}`}>
            {String(value)}
        </span>
    );
}
