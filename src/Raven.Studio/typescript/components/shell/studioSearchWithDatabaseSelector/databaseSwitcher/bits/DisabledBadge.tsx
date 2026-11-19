import Badge from "react-bootstrap/Badge";
import React from "react";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function DisabledBadge({ isDisabled }: { isDisabled: boolean }) {
    const t = useStudioTranslation("studioSearchWithDatabaseSelector");

    if (!isDisabled) {
        return null;
    }

    return (
        <Badge className="ms-2" pill bg="secondary">
            {t("databaseSwitcher.disabled")}
        </Badge>
    );
}
