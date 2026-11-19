import { useAppUrls } from "hooks/useAppUrls";
import { EmptySet } from "components/common/EmptySet";
import React from "react";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function NoIndexes() {
    const t = useStudioTranslation("indexesList");
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { forCurrentDatabase } = useAppUrls();

    return (
        <div className="text-center">
            <EmptySet>{t("empty.title")}</EmptySet>

            {hasDatabaseWriteAccess && (
                <div className="vstack align-items-center gap-2 mt-2">
                    <a href={forCurrentDatabase.newIndex()} className="btn btn-outline-primary">
                        {t("empty.createNewIndex")}
                    </a>
                    <a href={forCurrentDatabase.indexes(null, null, true)()} className="btn btn-outline-info">
                        {t("empty.importIndexes")}
                    </a>
                </div>
            )}
        </div>
    );
}
