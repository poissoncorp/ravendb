import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ClusterDebugAboutView() {
    const t = useStudioTranslation("clusterDebug");

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>{t("about.intro")}</p>

                <p>
                    <StudioTrans
                        ns="clusterDebug"
                        i18nKey="about.summary"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                    <br />
                    <br />
                    <StudioTrans
                        ns="clusterDebug"
                        i18nKey="about.logEntries"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
