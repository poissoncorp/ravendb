import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import useConnectionStringsLicense from "components/pages/database/settings/connectionStrings/useConnectionStringsLicense";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ServerWideConnectionStringsInfoHub() {
    const t = useStudioTranslation("serverWideConnectionStrings");
    const commonT = useStudioTranslation("common");
    const { hasAll, featureAvailability } = useConnectionStringsLicense();
    const hasServerWideConnectionStrings = useAppSelector(
        licenseSelectors.statusValue("HasServerWideConnectionStrings")
    );
    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.serverWide"),
            featureIcon: "manage-connection-strings",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];

    const serverWideFeatureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasServerWideConnectionStrings,
            },
        ],
    });

    const isLicenseUnlimited = hasAll && hasServerWideConnectionStrings;

    const connectionStringsOverviewDocsLink = useRavenLink({ hash: "P5XJOV" });
    const connectionStringsServerwideDocsLink = useRavenLink({ hash: "5AQ7XL" });

    return (
        <AboutViewAnchored defaultOpen={isLicenseUnlimited ? null : "licensing"}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading={t("about.heading")}
                description={t("about.description")}
            >
                <div>
                    <ul>
                        <li>{t("about.manageItem")}</li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="serverWideConnectionStrings"
                                i18nKey="about.propagationItem"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">{t("about.ongoingTasksItem")}</li>
                        <li className="margin-top-xxs">{t("about.deleteItem")}</li>
                        <li className="margin-top-xxs">{t("about.exportItem")}</li>
                    </ul>
                    <hr />
                    <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                    <a href={connectionStringsOverviewDocsLink} target="_blank">
                        <Icon icon="newtab" /> {t("about.overviewDocsLink")}
                    </a>
                    <br />
                    <a href={connectionStringsServerwideDocsLink} target="_blank">
                        <Icon icon="newtab" /> {t("about.serverWideDocsLink")}
                    </a>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={[...serverWideFeatureAvailability, ...featureAvailability]}
                isUnlimited={isLicenseUnlimited}
                isOpenedByDefault={!hasServerWideConnectionStrings}
            />
        </AboutViewAnchored>
    );
}
