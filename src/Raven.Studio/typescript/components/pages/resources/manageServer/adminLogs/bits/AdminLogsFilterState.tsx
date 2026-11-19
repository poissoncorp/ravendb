import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AdminLogsFilterState({ isActive }: { isActive: boolean }) {
    const t = useStudioTranslation("adminLogs");

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive,
                    message: t("filterState.active"),
                },
                {
                    isActive: !isActive,
                    message: t("filterState.notActive"),
                },
            ]}
        >
            <Icon icon="filter" color={isActive ? "primary" : "muted"} />
        </ConditionalPopover>
    );
}
