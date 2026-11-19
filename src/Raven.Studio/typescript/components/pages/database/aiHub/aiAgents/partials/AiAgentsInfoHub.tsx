import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { Icon } from "components/common/Icon";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AiAgentsInfoHub() {
    const hasAiAgent = useAppSelector(licenseSelectors.statusValue("HasAiAgent"));
    const aiAgentsOverviewDocsLink = useRavenLink({ hash: "XNWAXB" });
    const t = useStudioTranslation("aiAgents");
    const commonT = useStudioTranslation("common");

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.aiAgents"),
            featureIcon: "ai-agents",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: false },
            enterpriseAi: { value: true },
            quill: { value: true },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasAiAgent,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasAiAgent ? null : "licensing"}>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>
                            <StudioTrans ns="aiAgents" i18nKey="about.intro" components={{ strong: <strong /> }} />
                        </li>
                        <li className="mt-1">{t("about.conversations")}</li>
                        <li className="mt-1">
                            <StudioTrans ns="aiAgents" i18nKey="about.thisView" components={{ br: <br /> }} />
                        </li>
                        <li className="mt-1">
                            <StudioTrans ns="aiAgents" i18nKey="about.startChat" components={{ br: <br /> }} />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={aiAgentsOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasAiAgent} data={featureAvailability} />
        </AboutViewFloating>
    );
}
