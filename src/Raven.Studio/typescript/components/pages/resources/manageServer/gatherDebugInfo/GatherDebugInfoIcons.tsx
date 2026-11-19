import { Icon } from "components/common/Icon";
import React from "react";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "hooks/useStudioTranslation";

function GatherDebugInfoIcons() {
    const t = useStudioTranslation("gatherDebugInfo");
    const icons: IconName[] = ["replication", "stats", "io-test", "storage", "memory", "other"];
    const labels = [
        t("icons.replication"),
        t("icons.performance"),
        t("icons.io"),
        t("icons.storage"),
        t("icons.memory"),
        t("icons.other"),
    ];
    return (
        <div className="d-flex flex-row my-3 gap-4 flex-wrap justify-content-center icons-list">
            {icons.map((icon, index) => (
                <div key={icon} className="d-flex flex-column align-items-center text-center gap-3">
                    <Icon icon={icon} margin="m-0" />
                    <p>{labels[index]}</p>
                </div>
            ))}
        </div>
    );
}

export default GatherDebugInfoIcons;
