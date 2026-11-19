import Button from "react-bootstrap/Button";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface LazyVirtualTableDomLimitBannerProps {
    itemsName: string;
    canPaginate: boolean;
    onTurnOnPagination: () => void;
}

export default function LazyVirtualTableDomLimitBanner({
    itemsName,
    canPaginate,
    onTurnOnPagination,
}: LazyVirtualTableDomLimitBannerProps) {
    return (
        <div className="floating-bar text-nowrap" data-testid="dom-limit-banner">
            <span>
                <StudioTrans
                    ns="virtualTable"
                    i18nKey="table.domLimit"
                    components={{
                        button: <Button variant="link" className="p-0 align-baseline" onClick={onTurnOnPagination} />,
                    }}
                    options={{ context: canPaginate ? "canPaginate" : undefined, itemsName }}
                />
            </span>
        </div>
    );
}
