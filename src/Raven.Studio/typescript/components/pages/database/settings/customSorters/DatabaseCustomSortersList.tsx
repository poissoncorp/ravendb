import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import DatabaseCustomSortersListItem from "components/pages/database/settings/customSorters/DatabaseCustomSortersListItem";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";
import { CustomSorter } from "components/common/customSorters/useCustomSorters";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseCustomSortersListProps {
    sorters: CustomSorter[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    serverWideSorterNames: string[];
    takenNames: string[];
    remove: (idx: number) => void;
    markAsSaved: (idx: number, name: string) => void;
}

export default function DatabaseCustomSortersList(props: DatabaseCustomSortersListProps) {
    const { sorters, fetchStatus, reload, remove, serverWideSorterNames, takenNames, markAsSaved } = props;
    const t = useStudioTranslation("customSorters");

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error={t("loadError")} refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>{t("empty")}</EmptySet>;
    }
    return sorters.map((sorter, idx) => (
        <DatabaseCustomSortersListItem
            key={sorter.id}
            initialSorter={sorter}
            takenNames={takenNames.filter((name) => name !== sorter.name)}
            serverWideSorterNames={serverWideSorterNames}
            remove={() => remove(idx)}
            markAsSaved={(name: string) => markAsSaved(idx, name)}
        />
    ));
}
