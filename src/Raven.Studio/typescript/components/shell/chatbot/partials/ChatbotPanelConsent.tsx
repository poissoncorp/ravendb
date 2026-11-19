import IconAsciiPlaceholder from "components/shell/chatbot/partials/askAi/iconAscii/IconAsciiPlaceholder";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ChatbotPanelConsent() {
    const t = useStudioTranslation("chatbot");

    return (
        <div>
            <IconAsciiPlaceholder />
            <h3 className="mt-4 mb-0 fw-semibold">{t("aiAssistant")}</h3>
            <div className="mt-2">
                <StudioTrans ns="chatbot" i18nKey="consent.description" components={{ br: <br /> }} />
                <div className="mt-3">
                    <Button variant="primary" className="rounded-pill">
                        {t("consent.reviewTermsOfUse")}
                    </Button>
                </div>
            </div>
        </div>
    );
}
