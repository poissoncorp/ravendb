import React from "react";
import "./SelectIndexDropdownToggle.scss";
import Badge from "react-bootstrap/Badge";
import { FormLabel } from "components/common/Form";
import { Checkbox } from "components/common/Checkbox";
import { FlexGrow } from "components/common/FlexGrow";
import { IndexErrorsDropdownType, NameAndCount } from "components/pages/database/indexes/errors/types";
import CheckboxSelectAll from "components/common/CheckboxSelectAll";
import { useCheckboxes } from "hooks/useCheckboxes";
import { useIndexErrorsDropdown } from "components/pages/database/indexes/errors/hooks/useIndexErrorsDropdown";
import { ColumnFiltersState, Updater } from "@tanstack/react-table";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface SelectIndexErrorsDropdownProps {
    indexesList: NameAndCount[];
    filters: ColumnFiltersState;
    setFilters: (updater: Updater<ColumnFiltersState>) => void;
    isLoading: boolean;
    labelContext: "indexes" | "actions";
    dropdownType: IndexErrorsDropdownType;
}

export function SelectIndexErrorsDropdown({
    indexesList,
    dropdownType,
    setFilters,
    filters,
    labelContext,
    isLoading,
}: SelectIndexErrorsDropdownProps) {
    const t = useStudioTranslation("indexErrors");
    const { setSelectedIndexes, labelText, selectedColumnFilters } = useIndexErrorsDropdown({
        filters,
        dropdownType,
        setFilters,
        labelContext,
    });

    const { selectionState, toggleOne, toggleAll } = useCheckboxes({
        allItems: indexesList.map((item) => item.name),
        selectedItems: selectedColumnFilters,
        setValue: setSelectedIndexes,
    });

    return (
        <Dropdown className="select-index-errors-dropdown">
            <ConditionalPopover
                conditions={{
                    isActive: indexesList.length === 0,
                    message: t("filter.noItems", { context: labelContext }),
                }}
            >
                <Dropdown.Toggle
                    as={CustomDropdownToggle}
                    variant="secondary"
                    disabled={isLoading || indexesList.length === 0}
                    className="select-index-errors-toggle d-flex align-items-center"
                    isCaretHidden
                >
                    <div className="flex-grow d-flex align-items-center">{labelText}</div>
                </Dropdown.Toggle>
            </ConditionalPopover>
            <Dropdown.Menu className="p-3 custom-dropdown-menu">
                <div className="vstack gap-2">
                    <div className="hstack lh-1 gap-3">
                        <CheckboxSelectAll
                            selectedItemsCount={selectedColumnFilters.length}
                            allItemsCount={indexesList.length}
                            color="primary"
                            selectionState={selectionState}
                            toggleAll={toggleAll}
                        />
                    </div>
                    <hr className="m-0" />
                    {indexesList.map((item, index) => (
                        <div className="hstack gap-2" key={index}>
                            <div
                                title={item.name}
                                className="hstack  lh-1 gap-3 dropdown-checkbox-group"
                                style={{ width: dropdownType === "Action" ? "260px" : "450px" }}
                            >
                                <Checkbox
                                    selected={selectedColumnFilters.includes(item.name)}
                                    toggleSelection={() => toggleOne(item.name)}
                                />
                                <FormLabel className="m-0 text-truncate">
                                    <small>{item.name}</small>
                                </FormLabel>
                            </div>
                            <FlexGrow />
                            <Badge bg="faded-danger">{item.count}</Badge>
                        </div>
                    ))}
                </div>
            </Dropdown.Menu>
        </Dropdown>
    );
}
