import React from "react";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import "./AnalysisProgress.scss";

interface AnalysisProgressProps {
    fileName?: string;
}

export default function AnalysisProgress({ fileName }: AnalysisProgressProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    return (
        <div className="analysis-progress">
            <Icon icon="search" className="fs-2" margin="m-0" />
            <div className="analysis-progress-text">
                <span className="analysis-progress-title">{t("analysisInProgress")}</span>
                {fileName && <span className="text-muted">{fileName}</span>}
            </div>
            <div
                className="analysis-progress-bar"
                role="progressbar"
                aria-label={t("analysisInProgress")}
                aria-busy="true"
            >
                <div className="analysis-progress-bar-fill" />
            </div>
        </div>
    );
}
