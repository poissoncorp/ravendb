import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditGenAiTaskInfoHub() {
    const t = useStudioTranslation("editGenAiTask");
    const commonT = useStudioTranslation("common");
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const hasGenAi = useAppSelector(licenseSelectors.statusValue("HasGenAi"));

    const genAiOverviewDocsLink = useRavenLink({ hash: "GEHC2I" });
    const genAiCreateTaskDocsLink = useRavenLink({ hash: "HTGCZZ" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.genAi"),
            featureIcon: "genai",
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
                value: hasGenAi,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasGenAi ? null : "licensing"}>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <p>
                        <StudioTrans
                            ns="editGenAiTask"
                            i18nKey="about.intro"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </p>
                    <hr />
                    {t("about.configurationIncludes")}
                    <br />
                    <br />
                    <p>
                        <StudioTrans
                            ns="editGenAiTask"
                            i18nKey="about.aiServiceAndModel"
                            components={{
                                strong: <strong />,
                                br: <br />,
                                a: <a href={appUrl.forAiConnectionStrings(activeDatabaseName)} target="_blank" />,
                            }}
                        />
                    </p>
                    <div>
                        <strong>{t("about.defineModelInput")}</strong>
                        <ul>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="editGenAiTask"
                                    i18nKey="about.contextObjects"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </li>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="editGenAiTask"
                                    i18nKey="about.prompt"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </li>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="editGenAiTask"
                                    i18nKey="about.jsonSchema"
                                    components={{ strong: <strong />, br: <br /> }}
                                />
                            </li>
                        </ul>
                    </div>
                    <p>
                        <StudioTrans
                            ns="editGenAiTask"
                            i18nKey="about.handleModelOutput"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </p>
                </div>
                <hr />
                <p>
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="about.testing"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="editGenAiTask"
                        i18nKey="about.ongoingProcessing"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={genAiOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.overviewDocsLink")}
                </a>
                <br />
                <a href={genAiCreateTaskDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.createTaskDocsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasGenAi} data={featureAvailability} />
        </AboutViewFloating>
    );
}
