import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AdminLogsPersistInfoIcon() {
    const t = useStudioTranslation("adminLogs");

    return (
        <PopoverWithHoverWrapper message={t("persistInfo")}>
            <Icon icon="info" color="info" margin="ms-1" />
        </PopoverWithHoverWrapper>
    );
}
