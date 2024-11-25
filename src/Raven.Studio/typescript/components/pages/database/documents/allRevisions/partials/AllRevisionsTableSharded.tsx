import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useVirtualTableWithToken } from "components/common/virtualTable/hooks/useVirtualTableWithToken";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import useAllRevisionsRowSelection from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsRowSelection";
import { useAppSelector } from "components/store";
import { useImperativeHandle, useMemo } from "react";

export default function AllRevisionsTableSharded({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    setSelectedRows,
}: AllRevisionsTableProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const { dataArray, componentProps, reload } = useVirtualTableWithToken({
        fetchData: (skip: number, take: number, continuationToken?: string) =>
            databasesService.getRevisionsPreview({
                databaseName,
                start: skip,
                pageSize: take,
                continuationToken,
                type: selectedType,
                collection: selectedCollectionName,
            }),
        dependencies: [databaseName, selectedType, selectedCollectionName],
    });

    const columns = useMemo(() => allRevisionsUtils.getColumnDefs(databaseName, true, width), [databaseName, width]);
    const { rowSelection, setRowSelection } = useAllRevisionsRowSelection({ dataPreview: dataArray, setSelectedRows });

    useImperativeHandle(fetcherRef, () => ({
        reload: async () => {
            await reload();
            setRowSelection([]);
        },
    }));

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
        },
        state: {
            rowSelection,
        },
        columns,
        data: dataArray,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        onRowSelectionChange: setRowSelection,
    });

    return <VirtualTable {...componentProps} table={table} heightInPx={height} />;
}
