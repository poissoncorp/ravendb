import { UseAsyncReturn } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import { EmptySet } from "components/common/EmptySet";
import React from "react";
import { RichPanel, RichPanelHeader, RichPanelInfo, RichPanelName } from "components/common/RichPanel";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseCustomAnalyzersServerWideListProps {
    asyncGetAnalyzers: UseAsyncReturn<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[], any[]>;
}

export default function DatabaseCustomAnalyzersServerWideList({
    asyncGetAnalyzers,
}: DatabaseCustomAnalyzersServerWideListProps) {
    const t = useStudioTranslation("databaseCustomAnalyzers");

    if (asyncGetAnalyzers.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGetAnalyzers.status === "error") {
        return <LoadError error={t("loadError")} refresh={asyncGetAnalyzers.execute} />;
    }

    if (asyncGetAnalyzers.result.length === 0) {
        return <EmptySet>{t("serverWideEmpty")}</EmptySet>;
    }

    return (
        <div>
            {asyncGetAnalyzers.result.map((analyzer) => (
                <RichPanel key={analyzer.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{analyzer.Name}</RichPanelName>
                        </RichPanelInfo>
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
