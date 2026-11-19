import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import React from "react";
import { UseAsyncReturn } from "react-async-hook";
import DatabaseCustomSortersServerWideListItem from "components/pages/database/settings/customSorters/DatabaseCustomSortersServerWideListItem";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseCustomSortersServerWideListProps {
    asyncGetSorters: UseAsyncReturn<Raven.Client.Documents.Queries.Sorting.SorterDefinition[], any[]>;
}

export default function DatabaseCustomSortersServerWideList({
    asyncGetSorters,
}: DatabaseCustomSortersServerWideListProps) {
    const t = useStudioTranslation("customSorters");

    if (asyncGetSorters.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetSorters.status === "error") {
        return <LoadError error={t("loadError")} refresh={asyncGetSorters.execute} />;
    }

    if (asyncGetSorters.result.length === 0) {
        return <EmptySet>{t("serverWideEmpty")}</EmptySet>;
    }

    return (
        <div>
            {asyncGetSorters.result.map((sorter) => (
                <DatabaseCustomSortersServerWideListItem key={sorter.Name} sorter={sorter} />
            ))}
        </div>
    );
}
