import { RichPanelStatus } from "components/common/RichPanel";
import { CertificatesState } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { TranslationKey } from "common/i18n/resources";

interface CertificatesItemStatusProps {
    state: CertificatesState;
}

const STATUS_TEXT_KEYS = {
    Valid: "state.valid",
    "About to expire": "state.valid",
    Expired: "state.expired",
    Disabled: "state.disabled",
} as const satisfies Record<CertificatesState, TranslationKey<"certificates">>;

export default function CertificatesItemStatus({ state }: CertificatesItemStatusProps) {
    const t = useStudioTranslation("certificates");
    const statusText = t(STATUS_TEXT_KEYS[state]);

    return <RichPanelStatus color={certificatesUtils.getStateColor(state)}>{statusText}</RichPanelStatus>;
}
