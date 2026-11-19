import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function OptionalLabel() {
    const t = useStudioTranslation("commonComponents");

    return <small className="fw-light">{t("optional")}</small>;
}
