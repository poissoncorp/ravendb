import { useMemo } from "react";
import useDialog from "components/common/Dialog";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { GenAiAiAttachment } from "../utils/editGenAiTaskValidation";
import genUtils from "common/generalUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import SizeGetter from "components/common/SizeGetter";
import {
    useReactTable,
    getCoreRowModel,
    getSortedRowModel,
    getFilteredRowModel,
    ColumnDef,
} from "@tanstack/react-table";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import CellValue, { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import RichAlert from "components/common/RichAlert";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import classNames from "classnames";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

export default function EditGenAiTaskAttachmentsButton({ attachments }: { attachments: GenAiAiAttachment[] }) {
    const t = useStudioTranslation("editGenAiTask");
    const dialog = useDialog();

    if (!attachments || attachments.length === 0) {
        return null;
    }

    const hasNotFound = attachments.some((attachment) => attachment.Source === "NotFound");

    const showAttachments = async () => {
        await dialog({
            title: (
                <span>
                    <Icon icon="attachment" />
                    {t("attachments.title")}
                </span>
            ),
            message: (
                <SizeGetter
                    render={({ width }) => (
                        <AttachmentsModalBody
                            attachments={attachments}
                            availableWidthInPx={width}
                            hasNotFound={hasNotFound}
                        />
                    )}
                />
            ),
            modalSize: "lg",
        });
    };

    return (
        <Button
            variant={hasNotFound ? "warning" : "success"}
            title={t("attachments.title")}
            onClick={showAttachments}
            size="xs"
            className="rounded-2"
        >
            <Icon icon="attachment" />
            {t("attachments.seeAttachments", {
                count: attachments.length,
                formattedCount: genUtils.formatNumberToStringFixed(attachments.length, 0),
            })}
            {hasNotFound && <Icon icon="warning" className="ms-1" />}
        </Button>
    );
}

interface AttachmentsModalBodyProps {
    attachments: GenAiAiAttachment[];
    availableWidthInPx: number;
    hasNotFound: boolean;
}

function AttachmentsModalBody({ attachments, availableWidthInPx, hasNotFound }: AttachmentsModalBodyProps) {
    const t = useStudioTranslation("editGenAiTask");
    const sortedAttachments = useMemo(
        () =>
            [...attachments].sort((a, b) => {
                if (a.Source === "NotFound" && b.Source !== "NotFound") {
                    return -1;
                }
                if (a.Source !== "NotFound" && b.Source === "NotFound") {
                    return 1;
                }
                return a.Name.localeCompare(b.Name);
            }),
        [attachments]
    );

    const columns = useMemo(() => getAttachmentsColumns(availableWidthInPx, t), [availableWidthInPx, t]);
    const heightInPx = useMemo(() => virtualTableUtils.getHeightInPx(attachments.length, 400), [attachments.length]);

    const table = useReactTable({
        columns,
        data: sortedAttachments,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return (
        <div>
            {hasNotFound && (
                <RichAlert variant="warning" className="mb-2">
                    {t("attachments.someNotFound")}
                </RichAlert>
            )}
            <VirtualTable heightInPx={heightInPx} table={table} />
        </div>
    );
}

const getAttachmentsColumns = (
    availableWidthInPx: number,
    t: StudioTranslate<"editGenAiTask">
): ColumnDef<GenAiAiAttachment>[] => {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidthInPx);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    return [
        {
            accessorKey: "Name",
            header: t("attachments.columns.name"),
            cell: ({ getValue, row }) => {
                const isFromUser = row.getValue<string>("Source") === "FromUser";
                return <CellValue value={isFromUser ? "" : getValue()} />;
            },
            size: getSize(20),
        },
        {
            accessorKey: "Source",
            header: t("attachments.columns.source"),
            cell: ({ getValue }) => (
                <CellValue value={getValue()} className={classNames({ "text-warning": getValue() === "NotFound" })} />
            ),
            size: getSize(20),
        },
        {
            accessorKey: "Type",
            header: t("attachments.columns.type"),
            cell: CellValueWrapper,
            size: getSize(20),
        },
        {
            accessorKey: "Data",
            header: t("attachments.columns.data"),
            cell: CellWithCopyWrapper,
            size: getSize(30),
        },
        {
            accessorKey: "RemoteStorageId",
            cell: CellValueWrapper,
            size: getSize(10),
        },
    ];
};
