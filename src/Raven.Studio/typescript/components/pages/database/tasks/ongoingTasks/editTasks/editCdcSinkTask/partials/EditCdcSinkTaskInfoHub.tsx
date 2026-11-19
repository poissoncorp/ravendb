import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { FeatureAvailabilityData } from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditCdcSinkTaskInfoHub() {
    const t = useStudioTranslation("editCdcSinkTask");
    const commonT = useStudioTranslation("common");
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const hasCdcSink = useAppSelector(licenseSelectors.statusValue("HasCdcSink"));

    const cdcSinkOverviewDocsLink = useRavenLink({ hash: "36RFXH" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.cdcSinkTask"),
            featureIcon: "cdc-sink",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            enterpriseAi: { value: true },
            quill: { value: true },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasCdcSink,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasCdcSink ? null : "licensing"}>
            <AccordionItemWrapper
                icon="about"
                color="info"
                heading={t("about.heading")}
                description={t("about.description")}
                targetId="about-view"
            >
                <div>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="about.intro"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                    <hr />
                    {t("about.configurationIncludes")}
                    <ul>
                        <li className="mt-1">
                            <StudioTrans
                                ns="editCdcSinkTask"
                                i18nKey="about.connectionStringItem"
                                components={{
                                    a: (
                                        <a
                                            href={appUrl.forConnectionStrings(activeDatabaseName, "Sql")}
                                            target="_blank"
                                        />
                                    ),
                                }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="editCdcSinkTask"
                                i18nKey="about.discoverTablesItem"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li>
                            <strong>{t("about.configureTablesItem")}</strong>
                            <ul>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="about.targetCollectionItem"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="about.fieldMappingsItem"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="about.documentIdItem"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="about.relatedTablesItem"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                    <ul>
                                        <li>
                                            <StudioTrans
                                                ns="editCdcSinkTask"
                                                i18nKey="about.embeddedTableItem"
                                                components={{ strong: <strong /> }}
                                            />
                                        </li>
                                        <li>
                                            <StudioTrans
                                                ns="editCdcSinkTask"
                                                i18nKey="about.linkedTableItem"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                    </ul>
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="about.onDeleteItem"
                                        components={{ strong: <strong />, br: <br /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="about.patchScriptsItem"
                                        components={{ strong: <strong />, br: <br />, em: <em /> }}
                                    />
                                </li>
                            </ul>
                        </li>
                    </ul>
                </div>
                <hr />
                <p>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="about.testing"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="about.initialLoad"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="editCdcSinkTask"
                        i18nKey="about.ongoingProcessing"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={cdcSinkOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasCdcSink} data={featureAvailability} />
        </AboutViewFloating>
    );
}
