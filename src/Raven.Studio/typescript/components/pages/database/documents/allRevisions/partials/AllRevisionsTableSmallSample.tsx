import { useReactTable, getCoreRowModel } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useServices } from "components/hooks/useServices";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import useAllRevisionsRowSelection from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsRowSelection";
import { useAppSelector } from "components/store";
import { useImperativeHandle, useMemo } from "react";
import { useAsync } from "react-async-hook";

export default function AllRevisionsTableSmallSample({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    setSelectedRows,
}: AllRevisionsTableProps) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { databasesService } = useServices();

    const asyncGetRevisionsPreview = useAsync(
        () =>
            databasesService.getRevisionsPreview({
                databaseName: db.name,
                start: 0,
                pageSize: allRevisionsUtils.smallSampleSize,
                type: selectedType,
                collection: selectedCollectionName,
            }),
        [db.name, selectedType, selectedCollectionName, allRevisionsUtils.smallSampleSize]
    );

    const columns = useMemo(
        () => allRevisionsUtils.getColumnDefs(db.name, db.isSharded, width),
        [db.name, db.isSharded, width]
    );

    const data = useMemo(() => asyncGetRevisionsPreview.result?.items ?? [], [asyncGetRevisionsPreview.result]);

    const { rowSelection, setRowSelection } = useAllRevisionsRowSelection({ dataPreview: data, setSelectedRows });

    useImperativeHandle(fetcherRef, () => ({
        reload: async () => {
            await asyncGetRevisionsPreview.execute();
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
        data,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        onRowSelectionChange: setRowSelection,
    });

    return <VirtualTable table={table} heightInPx={height} />;
}
