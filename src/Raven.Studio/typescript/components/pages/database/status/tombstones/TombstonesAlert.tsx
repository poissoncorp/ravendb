import React from "react";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function TombstonesAlert() {
    const t = useStudioTranslation("tombstones");

    return (
        <RichAlert variant="info" className="mb-0">
            <p>{t("alert.schedule")}</p>
            <p className="mb-0">
                <StudioTrans ns="tombstones" i18nKey="alert.forceCleanup" components={{ strong: <strong /> }} />
            </p>
        </RichAlert>
    );
}
