import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AdminLogsConfigTableValue({ value }: { value: unknown }) {
    const t = useStudioTranslation("adminLogs");

    if (value == null) {
        return <Icon icon="minus" color="muted" title={t("tableValue.null")} />;
    }
    if (value === false) {
        return <Icon icon="cancel" color="danger" title={t("tableValue.false")} />;
    }
    if (value === true) {
        return <Icon icon="check" color="success" title={t("tableValue.true")} />;
    }

    return String(value);
}
