import { ReactNode } from "react";
import Spinner from "react-bootstrap/Spinner";
import { EmptySet } from "../../EmptySet";
import { virtualTableConstants } from "../utils/virtualTableConstants";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export interface VirtualTableStateProps {
    isLoading: boolean;
    isEmpty: boolean;
    emptyMessage?: ReactNode;
    isCompact?: boolean;
}

export function VirtualTableState(props: VirtualTableStateProps) {
    const t = useStudioTranslation("virtualTable");
    const { isLoading, isEmpty, emptyMessage = t("table.noResults"), isCompact } = props;

    const headerHeightInPx = isCompact
        ? virtualTableConstants.compactHeaderHeightInPx
        : virtualTableConstants.headerHeightInPx;
    const bodyCenterStyle = { top: `calc(50% + ${headerHeightInPx / 2}px)` };

    return (
        <>
            {isLoading && (
                <Spinner className="spinner-gradient table-state" style={bodyCenterStyle} data-testid="loader" />
            )}
            {isEmpty && !isLoading && (
                <div className="table-state" style={bodyCenterStyle}>
                    <EmptySet compact>{emptyMessage}</EmptySet>
                </div>
            )}
        </>
    );
}
