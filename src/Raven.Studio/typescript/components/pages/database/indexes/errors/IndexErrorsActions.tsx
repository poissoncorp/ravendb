import React from "react";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { NameAndCount } from "components/pages/database/indexes/errors/types";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import { SelectIndexErrorsDropdown } from "components/pages/database/indexes/errors/SelectIndexErrorsDropdown";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface FilterIndexErrorProps {
    erroredIndexNames: NameAndCount[];
    erroredActionNames: NameAndCount[];
    refresh: () => Promise<Awaited<ErrorInfoItem>[]>;
    isLoading: boolean;
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
}

export default function IndexErrorsActions({
    erroredIndexNames,
    filters,
    setFilters,
    erroredActionNames,
    refresh,
    isLoading,
}: FilterIndexErrorProps) {
    const t = useStudioTranslation("indexErrors");
    const commonT = useStudioTranslation("common");

    return (
        <div className="hstack flex-wrap align-items-end gap-2 mb-3 justify-content-end">
            <div title={t("filter.indexesTitle")}>
                <SelectIndexErrorsDropdown
                    indexesList={erroredIndexNames}
                    isLoading={isLoading}
                    filters={filters}
                    setFilters={setFilters}
                    labelContext="indexes"
                    dropdownType="IndexName"
                />
            </div>
            <div title={t("filter.actionsTitle")}>
                <SelectIndexErrorsDropdown
                    indexesList={erroredActionNames}
                    isLoading={isLoading}
                    filters={filters}
                    setFilters={setFilters}
                    labelContext="actions"
                    dropdownType="Action"
                />
            </div>
            <ButtonWithSpinner onClick={refresh} icon="refresh" isSpinning={isLoading} variant="primary">
                {commonT("refresh")}
            </ButtonWithSpinner>
        </div>
    );
}
