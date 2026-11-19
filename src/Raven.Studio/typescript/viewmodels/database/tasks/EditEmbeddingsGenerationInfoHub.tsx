import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useAppUrls } from "hooks/useAppUrls";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { aiConnectionStringUtils } from "components/pages/database/settings/connectionStrings/editForms/aiConnectionStringUtils";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditEmbeddingsGenerationInfoHub() {
    const t = useStudioTranslation("editEmbeddingsGenerationTask");
    const commonT = useStudioTranslation("common");
    const hasEmbeddingsGeneration = useAppSelector(licenseSelectors.statusValue("HasEmbeddingsGeneration"));

    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const generatingEmbeddingsOverviewDocsLink = useRavenLink({ hash: "H6GDNY" });
    const embeddingsGenerationTaskDocsLink = useRavenLink({ hash: "MKXRLR" });
    const chunkingMethodsDocsLink = useRavenLink({ hash: "TLJF56" });
    const quantizationOptionsDocsLink = useRavenLink({ hash: "JWJPZE" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.embeddedModel"),
            featureIcon: "ai-etl",
            community: { value: true },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
            helperInfo: "bge-micro-v2",
        },
        {
            featureName: t("featureAvailability.externalModels"),
            featureIcon: "ai-etl",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
            helperInfo: (
                <ul>
                    {aiConnectionStringUtils.getExternalConnectorOptions().map((x) => (
                        <li key={x.value}>{x.label}</li>
                    ))}
                </ul>
            ),
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasEmbeddingsGeneration,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasEmbeddingsGeneration ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <p>
                        <StudioTrans
                            ns="editEmbeddingsGenerationTask"
                            i18nKey="about.intro"
                            components={{ br: <br /> }}
                        />
                    </p>
                    <hr />
                    {t("about.configurationIncludes")}
                    <br />
                    <br />
                    <p>
                        <StudioTrans
                            ns="editEmbeddingsGenerationTask"
                            i18nKey="about.destination"
                            components={{
                                strong: <strong />,
                                br: <br />,
                                a: <a href={appUrl.forAiConnectionStrings(activeDatabaseName)} target="_blank" />,
                            }}
                        />
                    </p>
                    <p>
                        <StudioTrans
                            ns="editEmbeddingsGenerationTask"
                            i18nKey="about.sourceData"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </p>
                    <p>
                        <StudioTrans
                            ns="editEmbeddingsGenerationTask"
                            i18nKey="about.quantization"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </p>
                    <p>
                        <StudioTrans
                            ns="editEmbeddingsGenerationTask"
                            i18nKey="about.expiration"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </p>
                    <p>
                        <StudioTrans
                            ns="editEmbeddingsGenerationTask"
                            i18nKey="about.queryTime"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </p>
                </div>
                <hr />
                <div>
                    <StudioTrans
                        ns="editEmbeddingsGenerationTask"
                        i18nKey="about.ongoingProcessing"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={generatingEmbeddingsOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.overviewDocsLink")}
                </a>
                <br />
                <a href={embeddingsGenerationTaskDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.taskDocsLink")}
                </a>
                <br />
                <a href={chunkingMethodsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.chunkingDocsLink")}
                </a>
                <br />
                <a href={quantizationOptionsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.quantizationDocsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasEmbeddingsGeneration}
                data={featureAvailability}
                isOpenedByDefault={false}
            />
        </AboutViewFloating>
    );
}
