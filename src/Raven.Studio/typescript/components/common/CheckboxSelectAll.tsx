import { Checkbox, CheckboxProps } from "components/common/Checkbox";
import { SelectionState } from "components/models/common";
import React from "react";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface CheckboxSelectAllProps extends Pick<CheckboxProps, "size"> {
    selectionState: SelectionState;
    toggleAll: () => void;
    allItemsCount?: number;
    selectedItemsCount?: number;
    color?: string;
    title?: string;
}

export default function CheckboxSelectAll({
    selectionState,
    toggleAll,
    allItemsCount,
    selectedItemsCount,
    size = "md",
    color = "primary",
    title,
}: CheckboxSelectAllProps) {
    const t = useStudioTranslation("commonComponents");

    return (
        <Checkbox
            size={size}
            toggleSelection={toggleAll}
            indeterminate={selectionState === "SomeSelected"}
            selected={selectionState === "AllSelected"}
            title={title ?? t("selection.title")}
            color={color}
        >
            <span className="text-uppercase small-label">
                {selectionState === "Empty" ? (
                    <span>
                        {t("selection.selectAll")}{" "}
                        {allItemsCount ? <Badge bg="secondary">{allItemsCount}</Badge> : null}
                    </span>
                ) : (
                    <span>
                        {t("selection.deselectAll")}{" "}
                        {selectedItemsCount ? <Badge bg="secondary">{selectedItemsCount}</Badge> : null}
                    </span>
                )}
            </span>
        </Checkbox>
    );
}
