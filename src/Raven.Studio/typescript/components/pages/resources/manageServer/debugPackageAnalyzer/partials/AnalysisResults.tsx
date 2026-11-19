import React, { useLayoutEffect, useMemo, useRef, useState } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import NodeTagPill from "./NodeTagPill";
import { EmptySet } from "components/common/EmptySet";
import Button from "react-bootstrap/Button";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { InputItem } from "components/models/common";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import { FlatIssue, countBy, issueCategories, issueScopes, scopeLabel, summarizeIssues } from "./analyzerUtils";
import DebugPackageDetailsSheet from "./DebugPackageDetailsSheet";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import "./AnalysisResults.scss";

type IssueSeverity = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues.IssueSeverity;

const cardSeverities: IssueSeverity[] = ["Error", "Warning", "Info"];

const GROUP_ROW_CAP = 5;

interface AnalysisResultsProps {
    issues: FlatIssue[];
}

export default function AnalysisResults({ issues }: AnalysisResultsProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const [category, setCategory] = useState<string>("all");
    const [scope, setScope] = useState<string>("all");
    const [collapsedGroups, setCollapsedGroups] = useState<Set<IssueSeverity>>(() => new Set());
    const [showAllGroups, setShowAllGroups] = useState<Set<IssueSeverity>>(() => new Set());

    const filtered = useMemo(
        () =>
            issues.filter(
                (issue) =>
                    (category === "all" || issue.category === category) && (scope === "all" || issue.scope === scope)
            ),
        [issues, category, scope]
    );

    const categoryCounts = useMemo(() => countBy(issues, (i) => i.category), [issues]);
    const scopeCounts = useMemo(() => countBy(issues, (i) => i.scope), [issues]);
    const severityCounts = useMemo(() => summarizeIssues(filtered).counts, [filtered]);

    const categoryItems: InputItem<string>[] = [
        { label: t("analysisResults.all"), value: "all", count: issues.length },
        ...issueCategories.map((c) => ({
            label: t("analysisResults.category", { context: c }),
            value: c,
            count: categoryCounts[c] ?? 0,
        })),
    ];

    const scopeItems: InputItem<string>[] = [
        { label: t("analysisResults.all"), value: "all", count: issues.length },
        ...issueScopes.map((s) => ({ label: scopeLabel(s), value: s, count: scopeCounts[s] ?? 0 })),
    ];

    const visibleGroups = cardSeverities.filter((s) => (severityCounts[s] ?? 0) > 0);

    const toggleCollapsed = (severity: IssueSeverity) =>
        setCollapsedGroups((prev) => {
            const next = new Set(prev);
            if (next.has(severity)) {
                next.delete(severity);
            } else {
                next.add(severity);
            }
            return next;
        });

    const expandGroup = (severity: IssueSeverity) => setShowAllGroups((prev) => new Set(prev).add(severity));

    const collapseGroup = (severity: IssueSeverity) =>
        setShowAllGroups((prev) => {
            const next = new Set(prev);
            next.delete(severity);
            return next;
        });

    return (
        <div className="analysis-results">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack">
                    <div className="d-flex align-items-center justify-content-between mb-3">
                        <h3 className="mb-0">{t("analysisResults.title")}</h3>
                    </div>

                    <div className="d-flex gap-2 flex-wrap align-items-end mb-3">
                        <MultiRadioToggle<string>
                            label={t("analysisResults.filterByCategory")}
                            inputItems={categoryItems}
                            selectedItem={category}
                            setSelectedItem={setCategory}
                        />
                        <MultiRadioToggle<string>
                            label={t("analysisResults.filterByScope")}
                            inputItems={scopeItems}
                            selectedItem={scope}
                            setSelectedItem={setScope}
                        />
                    </div>

                    {visibleGroups.length === 0 ? (
                        <EmptySet>{t("analysisResults.empty")}</EmptySet>
                    ) : (
                        <div className="vstack gap-2">
                            {visibleGroups.map((severity) => (
                                <SeverityGroup
                                    key={severity}
                                    severity={severity}
                                    issues={filtered.filter((i) => i.severity === severity)}
                                    collapsed={collapsedGroups.has(severity)}
                                    showAll={showAllGroups.has(severity)}
                                    onToggleCollapse={() => toggleCollapsed(severity)}
                                    onShowAll={() => expandGroup(severity)}
                                    onShowLess={() => collapseGroup(severity)}
                                />
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

interface SeverityGroupProps {
    severity: IssueSeverity;
    issues: FlatIssue[];
    collapsed: boolean;
    showAll: boolean;
    onToggleCollapse: () => void;
    onShowAll: () => void;
    onShowLess: () => void;
}

function SeverityGroup({
    severity,
    issues,
    collapsed,
    showAll,
    onToggleCollapse,
    onShowAll,
    onShowLess,
}: SeverityGroupProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const meta = severityMeta(severity);
    const expandable = !collapsed && issues.length > GROUP_ROW_CAP;
    const capped = expandable && !showAll;
    // When capped, reveal one extra row so it peeks out from under the fade gradient that hosts the "Show all" button.
    const visible = collapsed ? [] : capped ? issues.slice(0, GROUP_ROW_CAP + 1) : issues;

    return (
        <div className="severity-group">
            <button type="button" className="issue-group-header" aria-expanded={!collapsed} onClick={onToggleCollapse}>
                <Icon icon={collapsed ? "chevron-right" : "chevron-down"} margin="m-0" />
                <span className="small-label">{t("analysisResults.group", { context: meta.group })}</span>
                <Badge bg="secondary" pill>
                    {issues.length}
                </Badge>
            </button>
            {!collapsed && (
                <div className={classNames("issue-list", { "issue-list--capped": capped })}>
                    {visible.map((issue) => (
                        <IssueRow key={issue.key} issue={issue} />
                    ))}
                    {capped && (
                        <div className="issue-list__fade">
                            <Button
                                variant="secondary"
                                size="sm"
                                className="issue-list__show-all rounded-pill"
                                onClick={onShowAll}
                            >
                                <Icon icon="expand-vertical" />
                                {t("analysisResults.showAll", { context: meta.group, count: issues.length })}
                            </Button>
                        </div>
                    )}
                    {expandable && showAll && (
                        <div className="d-flex justify-content-end">
                            <Button variant="link" size="xs" onClick={onShowLess}>
                                {t("analysisResults.showLess")}
                            </Button>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

function IssueRow({ issue }: { issue: FlatIssue }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const meta = severityMeta(issue.severity);

    return (
        <div className="issue-row d-flex gap-2 align-items-start">
            <Icon icon={meta.icon} color={meta.color} className="align-self-center" margin="m-0" />
            <div className="vstack flex-grow-1">
                <div className="issue-title fw-semibold">{issue.title}</div>
                <IssueDescription issue={issue} meta={meta} />
                {issue.recommendedAction && (
                    <div className="issue-recommendation">
                        <strong>{t("analysisResults.recommendedAction")}</strong> {issue.recommendedAction}
                    </div>
                )}
            </div>
            <ScopeBadges issue={issue} />
        </div>
    );
}

// Some descriptions (e.g. cluster connectivity alerts) embed full stack traces. The row stays compact with a
// 3-line clamp; "Show more" appears only when the text actually overflows, and opens the full text in a side sheet.
function IssueDescription({ issue, meta }: { issue: FlatIssue; meta: SeverityMeta }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const ref = useRef<HTMLDivElement>(null);
    const [overflowing, setOverflowing] = useState(false);
    const { open } = useViewSheet();
    const text = issue.description;

    useLayoutEffect(() => {
        const el = ref.current;
        if (el) {
            setOverflowing(el.scrollHeight > el.clientHeight + 1);
        }
    }, [text]);

    if (!text) {
        return null;
    }

    const showDetails = () =>
        open({
            component: (
                <DebugPackageDetailsSheet title={issue.title} content={text} icon={meta.icon} iconColor={meta.color} />
            ),
        });

    return (
        <div>
            <div ref={ref} className="issue-description text-muted clamped">
                {text}
            </div>
            {overflowing && (
                <Button variant="link" size="sm" className="p-0" onClick={showDetails}>
                    {t("analysisResults.showMore")}
                </Button>
            )}
        </div>
    );
}

function ScopeBadges({ issue }: { issue: FlatIssue }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    if (issue.scope === "cluster-wide") {
        return (
            <Badge bg="light" pill>
                <Icon icon="cluster" /> {t("analysisResults.clusterWide")}
            </Badge>
        );
    }

    const nodePills = issue.nodeTags.map((tag) => <NodeTagPill key={tag} tag={tag} />);

    if (issue.scope === "node") {
        return <div className="analysis-result-metadata">{nodePills}</div>;
    }

    return (
        <div className="analysis-result-metadata">
            <Badge bg="secondary" pill>
                <Icon icon="database" /> {issue.database}
            </Badge>
            {nodePills}
        </div>
    );
}

interface SeverityMeta {
    icon: IconName;
    color: ThemeColor;
    variant: "danger" | "warning" | "info" | "secondary";
    group: "error" | "warning" | "info" | "misc";
}

function severityMeta(severity: IssueSeverity): SeverityMeta {
    switch (severity) {
        case "Error":
            return { icon: "danger", color: "danger", variant: "danger", group: "error" };
        case "Warning":
            return { icon: "warning", color: "warning", variant: "warning", group: "warning" };
        case "Info":
            return { icon: "info", color: "info", variant: "info", group: "info" };
        default:
            return { icon: "info", color: "muted", variant: "secondary", group: "misc" };
    }
}
