import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import React from "react";
import { AsyncStateStatus } from "react-async-hook";
import ServerWideCustomSortersListItem from "components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSortersListItem";
import { CustomSorter } from "components/common/customSorters/useCustomSorters";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ServerWideCustomSortersListProps {
    sorters: CustomSorter[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    takenNames: string[];
    remove: (idx: number) => void;
    markAsSaved: (idx: number, name: string) => void;
}

export default function ServerWideCustomSortersList(props: ServerWideCustomSortersListProps) {
    const { sorters, fetchStatus, reload, remove, takenNames, markAsSaved } = props;
    const t = useStudioTranslation("serverWideSorters");

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
        <ServerWideCustomSortersListItem
            key={sorter.id}
            initialSorter={sorter}
            takenNames={takenNames.filter((name) => name !== sorter.name)}
            remove={() => remove(idx)}
            markAsSaved={(name: string) => markAsSaved(idx, name)}
        />
    ));
}
