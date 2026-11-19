import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function IntegrationsInfoHub() {
    const t = useStudioTranslation("integrations");
    const commonT = useStudioTranslation("common");
    const hasPostgreSql = useAppSelector(licenseSelectors.statusValue("HasPostgreSqlIntegration"));
    const hasPowerBi = useAppSelector(licenseSelectors.statusValue("HasPowerBI"));

    const integrationsDocsLink = useRavenLink({ hash: "ZE1BYH" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.postgreSql"),
            featureIcon: "postgresql",
            community: { value: true },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.powerBi"),
            featureIcon: "powerbi",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasPostgreSql,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasPowerBi,
            },
        ],
    });

    const hasAllFeaturesInLicense = hasPostgreSql && hasPowerBi;

    return (
        <AboutViewAnchored defaultOpen={hasAllFeaturesInLicense ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>{t("about.credentials")}</p>
                <p>{t("about.secureServer")}</p>
                <hr />
                <div>
                    {t("about.postgreSqlSupport")}
                    <ul>
                        <li className="margin-top-xxs">{t("about.postgreSqlProtocol")}</li>
                        <li className="margin-top-xxs">
                            <StudioTrans ns="integrations" i18nKey="about.verifyLicense" components={{ br: <br /> }} />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={integrationsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasAllFeaturesInLicense}
                data={featureAvailability}
                isOpenedByDefault={!hasPostgreSql && !hasPowerBi}
            />
        </AboutViewAnchored>
    );
}
