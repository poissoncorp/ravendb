import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import { InputItem } from "components/models/common";
import Select, { SelectOption } from "components/common/select/Select";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export type DocumentSchemaStatus = "enabled" | "disabled";

interface DocumentSchemaFilterProps {
    selectedCollections: SelectOption[];
    setSelectedCollections: (collections: SelectOption[]) => void;
    collectionOptions: SelectOption[];
    selectedStatuses: DocumentSchemaStatus[];
    setSelectedStatuses: (statuses: DocumentSchemaStatus[]) => void;
    schemasCount: number;
    isLoading?: boolean;
}

export default function DocumentSchemaFilter({
    selectedCollections,
    setSelectedCollections,
    collectionOptions,
    selectedStatuses,
    setSelectedStatuses,
    schemasCount,
    isLoading,
}: DocumentSchemaFilterProps) {
    const t = useStudioTranslation("documentSchema");

    const filterByStatusOptions: InputItem<DocumentSchemaStatus>[] = [
        { value: "enabled", label: t("status.enabled") },
        { value: "disabled", label: t("status.disabled") },
    ];

    return (
        <div className="hstack flex-wrap align-items-end gap-3 justify-content-end">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">{t("filter.collectionLabel")}</div>
                <Select
                    isLoading={isLoading}
                    options={collectionOptions}
                    isMulti
                    value={selectedCollections}
                    onChange={setSelectedCollections}
                    placeholder={t("filter.collectionPlaceholder")}
                    isClearable
                    isRoundedPill
                    className="filtering-input"
                    styles={{
                        container: (base) => ({
                            ...base,
                            minWidth: "300px",
                        }),
                    }}
                />
            </div>

            <MultiCheckboxToggle
                inputItems={filterByStatusOptions}
                label={t("filter.stateLabel")}
                selectedItems={selectedStatuses}
                setSelectedItems={setSelectedStatuses}
                selectAll
                selectAllLabel={t("filter.all")}
                selectAllCount={schemasCount}
            />
        </div>
    );
}
