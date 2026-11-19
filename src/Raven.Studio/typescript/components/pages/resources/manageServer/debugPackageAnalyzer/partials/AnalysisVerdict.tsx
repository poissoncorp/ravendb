import React from "react";
import { FlatIssue, summarizeIssues } from "./analyzerUtils";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";
import "./AnalysisVerdict.scss";

type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;
type VerdictVariant = "danger" | "warning" | "info" | "success";

const SEP = " · ";

interface VerdictMeta {
    bg: VerdictVariant;
    label: string;
}

function verdictMeta(worst: IssueSeverity, t: StudioTranslate<"debugPackageAnalyzer">): VerdictMeta {
    switch (worst) {
        case "Error":
            return { bg: "danger", label: t("analysisVerdict.needsAttention") };
        case "Warning":
            return { bg: "warning", label: t("analysisVerdict.reviewRecommended") };
        case "Info":
            return { bg: "info", label: t("analysisVerdict.informational") };
        default:
            return { bg: "success", label: t("analysisVerdict.healthy") };
    }
}

interface AnalysisVerdictProps {
    issues: FlatIssue[];
    onViewIssues?: () => void;
}

export default function AnalysisVerdict({ issues }: AnalysisVerdictProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { total, counts, worst } = summarizeIssues(issues);
    const meta = verdictMeta(worst, t);
    const healthy = total === 0;

    const segments = [
        { variant: "error", count: counts.Error },
        { variant: "warning", count: counts.Warning },
        { variant: "info", count: counts.Info },
    ].filter((segment) => segment.count > 0);

    const countParts = [
        counts.Error > 0 ? t("analysisVerdict.errors", { count: counts.Error }) : null,
        counts.Warning > 0 ? t("analysisVerdict.warnings", { count: counts.Warning }) : null,
        counts.Info > 0 ? t("analysisVerdict.info", { count: counts.Info }) : null,
    ].filter((part): part is string => part !== null);

    const caption = healthy ? t("analysisVerdict.noIssues") : `${countParts.join(SEP)}`;
    const meterLabel = healthy
        ? caption
        : t("analysisVerdict.findings", { count: total, details: countParts.join(", ") });

    return (
        <div className="analysis-verdict">
            <Badge bg={meta.bg} pill>
                {meta.label}
            </Badge>
            <div className="small-label">{caption}</div>
            <div className="analysis-verdict-body">
                <div className="severity-meter" role="img" aria-label={meterLabel}>
                    {healthy ? (
                        <span
                            className="severity-meter-segment severity-meter-segment--success"
                            style={{ width: "100%" }}
                            aria-hidden="true"
                        />
                    ) : (
                        segments.map((segment) => (
                            <span
                                key={segment.variant}
                                className={`severity-meter-segment severity-meter-segment--${segment.variant}`}
                                style={{ width: `${(segment.count / total) * 100}%` }}
                                aria-hidden="true"
                            />
                        ))
                    )}
                </div>
            </div>
        </div>
    );
}
