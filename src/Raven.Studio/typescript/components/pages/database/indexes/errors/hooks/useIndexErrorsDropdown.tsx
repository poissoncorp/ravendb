import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { useMemo } from "react";
import { IndexErrorsDropdownType } from "components/pages/database/indexes/errors/types";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface UseIndexErrorsDropdownProps {
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    dropdownType: IndexErrorsDropdownType;
    labelContext: "indexes" | "actions";
    filters: ColumnFiltersState;
}

export function useIndexErrorsDropdown({
    setFilters,
    dropdownType,
    filters,
    labelContext,
}: UseIndexErrorsDropdownProps) {
    const t = useStudioTranslation("indexErrors");
    const columnFilter = filters.find((filter) => filter.id === dropdownType);
    const selectedColumnFilters = (columnFilter?.value as string[]) ?? [];

    const setSelectedIndexes = (selected: string[]) => {
        setFilters((prev) => {
            const updatedFilters = prev.filter((filter) => filter.id !== dropdownType);
            if (selected.length > 0) {
                updatedFilters.push({ id: dropdownType, value: selected });
            }
            return updatedFilters;
        });
    };

    const labelText = useMemo(() => {
        if (selectedColumnFilters.length > 0) {
            return t("filter.selected", { context: labelContext, selectedCount: selectedColumnFilters.length });
        }

        return t("filter.allSelected", { context: labelContext });
    }, [selectedColumnFilters.length, labelContext, t]);

    return {
        labelText,
        setSelectedIndexes,
        selectedColumnFilters,
    };
}
