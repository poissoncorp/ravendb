import SizeGetter from "components/common/SizeGetter";
import { Label } from "reactstrap";
import Select from "components/common/select/Select";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import AllRevisionsWithSize from "components/pages/database/documents/allRevisions/partials/AllRevisionsWithSize";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import {
    OptionWithCount,
    SelectOptionWithCount,
    SingleValueWithCount,
} from "components/pages/database/documents/allRevisions/partials/AllRevisionsSelectComponents";
import useAllRevisionsFilters from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsFilters";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { useRef, useState } from "react";
import useConfirm from "components/common/ConfirmDialog";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import messagePublisher from "common/messagePublisher";
import { AllRevisionsFetcherRef } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";

type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export default function AllRevisions() {
    const { type, collection } = useAllRevisionsFilters();
    const [selectedRows, setSelectedRows] = useState<RevisionsPreviewResultItem[]>([]);

    const fetcherRef = useRef<AllRevisionsFetcherRef>(null);

    const confirm = useConfirm();
    const { databasesService } = useServices();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncRemoveRevisions = useAsyncCallback(async () => {
        const uniqueIds = Array.from(new Set(selectedRows.map((x) => x.Id)));

        for (const id of uniqueIds) {
            await databasesService.deleteRevisionsForDocuments(activeDatabaseName, {
                DocumentIds: [id],
                RevisionsChangeVectors: selectedRows.filter((x) => x.Id === id).map((x) => x.ChangeVector),
                RemoveForceCreatedRevisions: false,
            });
        }
        messagePublisher.reportSuccess(`Successfully removed ${selectedRows.length} revisions`);
    });

    const handleRemoveConfirmation = async () => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    Delete selected <strong>({selectedRows.length})</strong> revisions?
                </span>
            ),
            icon: "trash",
            actionColor: "danger",
            confirmText: "Delete",
        });

        if (isConfirmed) {
            await asyncRemoveRevisions.execute();
            await fetcherRef.current?.reload();
        }
    };

    return (
        <div className="content-padding vstack gap-2">
            <div className="d-flex gap-2 align-items-end">
                <div>
                    <Label className="small-label">Selected</Label>
                    <div>
                        <ButtonWithSpinner
                            color="danger"
                            onClick={handleRemoveConfirmation}
                            disabled={selectedRows.length === 0}
                            isSpinning={asyncRemoveRevisions.loading}
                            icon="trash"
                        >
                            Remove
                        </ButtonWithSpinner>
                    </div>
                </div>
                <div style={{ minWidth: 150 }}>
                    <Label className="small-label">Type</Label>
                    <Select
                        options={type.options}
                        isLoading={type.isLoading}
                        value={type.options.find((x) => x.value === type.value)}
                        onChange={(x: SelectOptionWithCount<RevisionType>) => type.setValue(x.value)}
                        components={{ Option: OptionWithCount, SingleValue: SingleValueWithCount }}
                    />
                </div>
                <div style={{ minWidth: 150 }}>
                    <Label className="small-label">Collection</Label>
                    <SelectCreatable
                        options={collection.options}
                        isLoading={collection.isLoading}
                        placeholder="Select collection..."
                        value={collection.options.find((x) => x.value === collection.value)}
                        onChange={(x: SelectOptionWithCount<string>) => collection.setValue(x?.value ?? "")}
                        isClearable
                        components={{ Option: OptionWithCount, SingleValue: SingleValueWithCount }}
                    />
                </div>
                {type.value !== "All" && collection.value && (
                    <RichAlert variant="warning">
                        The table contains only part of the results. When the selected revision type is other than
                        &quot;All&quot; and a collection is selected, only the first {allRevisionsUtils.smallSampleSize}{" "}
                        results are visible.
                    </RichAlert>
                )}
            </div>
            <SizeGetter
                isHeighRequired
                render={({ width, height }) => (
                    <AllRevisionsWithSize
                        width={width}
                        height={height}
                        selectedType={type.value}
                        selectedCollectionName={collection.value}
                        fetcherRef={fetcherRef}
                        setSelectedRows={setSelectedRows}
                    />
                )}
            />
        </div>
    );
}
