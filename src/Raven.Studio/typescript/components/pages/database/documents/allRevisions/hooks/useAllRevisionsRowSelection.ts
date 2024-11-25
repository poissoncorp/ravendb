import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { useState, useEffect } from "react";

export default function useAllRevisionsRowSelection({
    dataPreview,
    setSelectedRows,
}: {
    dataPreview: RevisionsPreviewResultItem[];
    setSelectedRows: (rows: RevisionsPreviewResultItem[]) => void;
}) {
    const [rowSelection, setRowSelection] = useState({});

    // Update selected rows
    useEffect(() => {
        const selectedRows = Object.entries(rowSelection)
            .filter((entry) => !!entry[1])
            .map(([key]) => dataPreview[parseInt(key)]);

        setSelectedRows(selectedRows);
    }, [dataPreview, rowSelection, setSelectedRows]);

    return {
        rowSelection,
        setRowSelection,
    };
}
