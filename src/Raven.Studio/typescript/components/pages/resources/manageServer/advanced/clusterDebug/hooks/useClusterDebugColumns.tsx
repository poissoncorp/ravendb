import { ColumnDef } from "@tanstack/react-table";
import genUtils from "common/generalUtils";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function useClusterDebugColumns(
    availableWidth: number,
    commitIndex: number,
    showInlinePreview: (logIndex: number) => void,
    deleteEntry: (logIndex: number) => void
) {
    const t = useStudioTranslation("clusterDebug");
    const commonT = useStudioTranslation("common");

    const columns: ColumnDef<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry>[] = [
        {
            header: t("entries.columns.preview"),
            cell: (context) => (
                <Button
                    title={t("entries.columns.previewTitle")}
                    variant="secondary"
                    onClick={() => showInlinePreview(context.row.original.Index)}
                    className={classNames({ invisible: context.row.original.SizeInBytes === 0 })}
                >
                    <Icon icon="preview" margin="m-0" />
                </Button>
            ),
            size: 70,
        },
        {
            header: t("entries.columns.index"),
            accessorKey: "Index",
            cell: CellWithCopyWrapper,
            size: 100,
        },
        {
            id: "commandType",
            header: t("entries.columns.commandType"),
            accessorKey: "CommandType",
            cell: CellWithCopyWrapper,
            size: 0, // see code below - we do here flex-grow
        },
        {
            header: t("entries.columns.created"),
            accessorKey: "CreateAt",
            cell: CellWithCopyWrapper,
            size: 250,
        },
        {
            header: t("entries.columns.size"),
            accessorFn: (row) => genUtils.formatBytesToSize(row.SizeInBytes),
            cell: CellWithCopyWrapper,
            size: 100,
        },
        {
            header: t("entries.columns.term"),
            accessorKey: "Term",
            cell: CellWithCopyWrapper,
            size: 70,
        },
        {
            header: t("entries.columns.status"),
            accessorFn: (row) =>
                row.Index <= commitIndex ? t("entries.columns.statusCommitted") : t("entries.columns.statusAppended"),
            cell: CellWithCopyWrapper,
            size: 150,
        },
        {
            header: commonT("delete"),
            cell: (context) => (
                <Button
                    title={t("entries.columns.deleteTitle")}
                    variant="danger"
                    onClick={() => deleteEntry(context.row.original.Index)}
                >
                    <Icon icon="trash" margin="m-0" />
                </Button>
            ),
            size: 80,
        },
    ];

    const allocatedSize = columns.reduce((p, c) => p + c.size, 0);
    const remainingSize = availableWidth - allocatedSize - padding;
    const commandTypeColumn = columns.find((x) => x.id === "commandType");
    if (commandTypeColumn) {
        commandTypeColumn.size = remainingSize;
    }

    return {
        columns,
    };
}

const padding = 32;
