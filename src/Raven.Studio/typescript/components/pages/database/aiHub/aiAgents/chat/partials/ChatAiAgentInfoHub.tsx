import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ChatAiAgentInfoHub() {
    const t = useStudioTranslation("aiAgentChat");

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>{t("about.chatView")}</li>
                        <li className="mt-1">{t("about.tools")}</li>
                        <li className="mt-1">{t("about.interaction")}</li>
                        <li className="mt-1">
                            <StudioTrans ns="aiAgentChat" i18nKey="about.storage" components={{ code: <code /> }} />
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
