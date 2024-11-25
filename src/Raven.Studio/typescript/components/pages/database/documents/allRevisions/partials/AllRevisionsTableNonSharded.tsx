import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useVirtualTableWithLazyLoading } from "components/common/virtualTable/hooks/useVirtualTableWithLazyLoading";
import VirtualTableWithLazyLoading from "components/common/virtualTable/VirtualTableWithLazyLoading";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import useAllRevisionsRowSelection from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsRowSelection";
import { useAppSelector } from "components/store";
import { useImperativeHandle, useMemo } from "react";

export default function AllRevisionsTableNonSharded({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    setSelectedRows,
}: AllRevisionsTableProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const { dataPreview, componentProps, reload } = useVirtualTableWithLazyLoading({
        fetchData: (skip: number, take: number) => {
            if (databaseName) {
                return databasesService.getRevisionsPreview({
                    databaseName,
                    start: skip,
                    pageSize: take,
                    type: selectedType,
                    collection: selectedCollectionName,
                });
            }
        },
        dependencies: [databaseName, selectedType, selectedCollectionName],
    });

    const columns = useMemo(() => allRevisionsUtils.getColumnDefs(databaseName, false, width), [databaseName, width]);
    const { rowSelection, setRowSelection } = useAllRevisionsRowSelection({ dataPreview, setSelectedRows });

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
        data: dataPreview,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        onRowSelectionChange: setRowSelection,
    });

    return <VirtualTableWithLazyLoading {...componentProps} table={table} heightInPx={height} />;
}
