import React from "react";
import Button from "react-bootstrap/Button";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "./Icon";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface LoadErrorProps {
    error?: string;
    refresh?: () => void;
}

export function LoadError(props: LoadErrorProps) {
    const { error, refresh } = props;
    const t = useStudioTranslation("commonComponents");
    const tCommon = useStudioTranslation("common");

    return (
        <RichAlert variant="danger">
            <div className="d-flex gap-1">
                <div>
                    <strong>{t("loadError.title")}</strong>
                    {error && <div>{error}</div>}
                </div>

                <FlexGrow />
                {refresh && (
                    <Button variant="secondary" onClick={refresh}>
                        <Icon icon="refresh" /> {tCommon("refresh")}
                    </Button>
                )}
            </div>
        </RichAlert>
    );
}
