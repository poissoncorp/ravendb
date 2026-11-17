import { AsyncStateStatus } from "react-async-hook";
import React from "react";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { EmptySet } from "components/common/EmptySet";
import { CustomAnalyzer } from "components/common/customAnalyzers/useCustomAnalyzers";
import DatabaseCustomAnalyzersListItem from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersListItem";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseCustomAnalyzersListProps {
    analyzers: CustomAnalyzer[];
    fetchStatus: AsyncStateStatus;
    reload: () => void;
    serverWideAnalyzerNames: string[];
    remove: (idx: number) => void;
    markAsSaved: (idx: number) => void;
}

export default function DatabaseCustomAnalyzersList(props: DatabaseCustomAnalyzersListProps) {
    const { analyzers, fetchStatus, reload, remove, serverWideAnalyzerNames, markAsSaved } = props;
    const t = useStudioTranslation("databaseCustomAnalyzers");

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error={t("loadError")} refresh={reload} />;
    }

    if (analyzers.length === 0) {
        return <EmptySet>{t("empty")}</EmptySet>;
    }

    return analyzers.map((analyzer, idx) => (
        <DatabaseCustomAnalyzersListItem
            key={analyzer.id}
            initialAnalyzer={analyzer}
            serverWideAnalyzerNames={serverWideAnalyzerNames}
            remove={() => remove(idx)}
            markAsSaved={() => markAsSaved(idx)}
        />
    ));
}
