import { SelectOption } from "components/common/select/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { SelectOptionWithCount } from "components/pages/database/documents/allRevisions/partials/AllRevisionsSelectComponents";
import { useAppSelector } from "components/store";
import { exhaustiveStringTuple } from "components/utils/common";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { useState } from "react";

type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export default function useAllRevisionsFilters() {
    const { databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const [selectedType, setSelectedType] = useState<RevisionType>("All");
    const [selectedCollectionName, setSelectedCollectionName] = useState("");

    const asyncGetTypeOptions = useAsyncDebounce(
        async (selectedCollectionName: string) => {
            const options: SelectOptionWithCount<RevisionType>[] = [];

            for (const type of exhaustiveStringTuple<RevisionType>()("All", "Regular", "Deleted")) {
                const baseOption: SelectOption<RevisionType> = { value: type, label: type };

                if (!selectedCollectionName) {
                    const previewResult = await databasesService.getRevisionsPreview({
                        databaseName,
                        start: 0,
                        pageSize: 0,
                        type,
                        collection: selectedCollectionName,
                    });

                    options.push({ ...baseOption, count: previewResult.totalResultCount });
                } else {
                    options.push({ ...baseOption, count: null });
                }
            }

            return options;
        },
        [selectedCollectionName]
    );
    const typeOptions = asyncGetTypeOptions.result ?? [];

    const asyncGetCollectionOptions = useAsyncDebounce(
        async (selectedType: RevisionType) => {
            const options: SelectOptionWithCount[] = [];

            for (const collectionName of allCollectionNames) {
                const baseOption: SelectOption = { label: collectionName, value: collectionName };

                if (selectedType === "All") {
                    const previewResult = await databasesService.getRevisionsPreview({
                        databaseName,
                        start: 0,
                        pageSize: 0,
                        type: selectedType,
                        collection: collectionName,
                    });

                    options.push({ ...baseOption, count: previewResult.totalResultCount });
                } else {
                    options.push({ ...baseOption, count: null });
                }
            }

            return options;
        },
        [selectedType]
    );
    const collectionOptions = asyncGetCollectionOptions.result ?? [];

    return {
        type: {
            options: typeOptions,
            isLoading: asyncGetTypeOptions.loading,
            value: selectedType,
            setValue: setSelectedType,
        },
        collection: {
            options: collectionOptions,
            isLoading: asyncGetCollectionOptions.loading,
            value: selectedCollectionName,
            setValue: setSelectedCollectionName,
        },
    };
}
