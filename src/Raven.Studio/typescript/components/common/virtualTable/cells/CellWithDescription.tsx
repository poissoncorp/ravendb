import { CellContext } from "@tanstack/react-table";
import copyToClipboard from "common/copyToClipboard";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import React from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type CellContextSubset<TData, TValue> = Pick<CellContext<TData, TValue>, "cell" | "getValue">;

export interface CellWithDescriptionProps<TData, TValue> extends CellContextSubset<TData, TValue> {
    description?: string;
    isActionsMenuVisible?: boolean;
    cellClassName?: string;
}

export function CellWithDescription<TData, TValue>({
    cell,
    getValue,
    description = String(getValue()),
    isActionsMenuVisible = true,
    cellClassName,
}: CellWithDescriptionProps<TData, TValue>) {
    const t = useStudioTranslation("virtualTable");
    const handleCopyToClipboard = () => {
        copyToClipboard.copy(String(getValue()), t("cells.itemCopied"));
    };

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    <div>{description}</div>
                    {isActionsMenuVisible && (
                        <>
                            <span className="small-label">{t("cells.actions")}</span>
                            <div>
                                <Button
                                    onClick={handleCopyToClipboard}
                                    variant="primary"
                                    title={t("cells.copyToClipboard")}
                                >
                                    <Icon icon="copy-to-clipboard" margin="m-0" />
                                </Button>
                            </div>
                        </>
                    )}
                </>
            }
        >
            <CellValueWrapper className={cellClassName} id={`popover-${cell.id}`} getValue={getValue} />
        </PopoverWithHoverWrapper>
    );
}
