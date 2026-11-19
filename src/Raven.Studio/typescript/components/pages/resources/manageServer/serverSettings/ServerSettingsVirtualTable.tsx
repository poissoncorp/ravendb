import React from "react";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import {
    ServerSettingsColumns,
    useServerSettingsColumns,
} from "components/pages/resources/manageServer/serverSettings/useServerSettingsColumns";
import { AsyncStateStatus } from "react-async-hook";
import { LoadError } from "components/common/LoadError";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ServerSettingsVirtualTableProps {
    height: number;
    width: number;
    data: ServerSettingsColumns[];
    isLoading: boolean;
    status: AsyncStateStatus;
    reload: () => void;
}

export function ServerSettingsVirtualTable({
    height,
    data,
    isLoading,
    reload,
    width,
    status,
}: ServerSettingsVirtualTableProps) {
    const t = useStudioTranslation("serverSettings");
    const serverSettingsColumns = useServerSettingsColumns(width);

    const serverSettingsTable = useReactTable({
        columnResizeMode: "onChange",
        columns: serverSettingsColumns,
        data,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
        initialState: {
            sorting: [
                {
                    id: "origin",
                    desc: true,
                },
            ],
        },
    });

    if (status === "error") {
        return <LoadError error={t("loadError")} refresh={reload} />;
    }

    return <VirtualTable className="mt-4" isLoading={isLoading} heightInPx={height} table={serverSettingsTable} />;
}
