import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function DatabaseLoadError(props: { error: string }) {
    const t = useStudioTranslation("databases");

    return (
        <PopoverWithHoverWrapper message={t("loadError.message", { error: props.error })}>
            <strong className="text-danger">
                <Icon icon="exclamation" /> {t("loadError.label")}
            </strong>
        </PopoverWithHoverWrapper>
    );
}
