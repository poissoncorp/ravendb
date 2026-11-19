import React from "react";
import { Switch } from "components/common/Checkbox";
import { useExpandAll } from "./ExpandAllContext";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

// Global switch that expands or collapses the per-node rows of every node-grouped analyzer table at
// once (Databases Overview, Storage per Database, Ongoing Tasks) via ExpandAllContext.
export default function ExpandAllToggle() {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { isAllExpanded, toggleAll } = useExpandAll();

    return (
        <Switch selected={isAllExpanded} toggleSelection={toggleAll} color="primary">
            {t("analysisView.expandAllNodeRows")}
        </Switch>
    );
}
