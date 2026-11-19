import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
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

export default function IndexCleanupAboutView() {
    const t = useStudioTranslation("indexCleanup");
    const commonT = useStudioTranslation("common");
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.indexCleanup"),
            featureIcon: "index-cleanup",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasIndexCleanup,
            },
        ],
    });

    const indexCleanupDocsLink = useRavenLink({ hash: "RWQW66" });

    return (
        <AboutViewFloating defaultOpen={hasIndexCleanup ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>{t("about.performance")}</p>
                <p className="mb-0">{t("about.recommendations")}</p>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={indexCleanupDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasIndexCleanup} data={featureAvailability} />
        </AboutViewFloating>
    );
}
