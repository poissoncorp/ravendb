import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { aiConnectionStringUtils } from "components/pages/database/settings/connectionStrings/editForms/aiConnectionStringUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function AiConnectionStringsInfoHub() {
    const t = useStudioTranslation("aiConnectionStrings");
    const commonT = useStudioTranslation("common");
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const aiConnectionStringsDocsLink = useRavenLink({ hash: "EAIKIA" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.ai"),
            featureIcon: "ai-etl",
            community: { value: true },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <ul>
                        <li>{t("about.intro")}</li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="aiConnectionStrings"
                                i18nKey="about.reuse"
                                components={{ a: <a href={appUrl.forAiTasks(activeDatabaseName)} target="_blank" /> }}
                            />
                        </li>
                        <li className="mt-1">
                            {t("about.supportedProviders")}
                            <ul>
                                {aiConnectionStringUtils.allConnectorOptions.map((option) => (
                                    <li key={option.value}>{option.label}</li>
                                ))}
                            </ul>
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={aiConnectionStringsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={true} data={defaultFeatureAvailability} />
        </AboutViewAnchored>
    );
}
