import DatabaseIcon from "./DatabaseIcon";
import React from "react";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function NoDatabasePlaceholder() {
    const t = useStudioTranslation("studioSearchWithDatabaseSelector");

    return (
        <span>
            <DatabaseIcon databaseName={null} />
            {t("databaseSwitcher.noDatabaseSelected")}
        </span>
    );
}
