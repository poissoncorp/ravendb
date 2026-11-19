import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditServerWideExternalReplicationInfoHub() {
    const t = useStudioTranslation("editServerWideExternalReplication");
    const hasServerWideExternalReplication = useAppSelector(licenseSelectors.statusValue("HasExternalReplication"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.serverWideExternalReplication"),
            featureIcon: "server-wide-replication",
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
                value: hasServerWideExternalReplication,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasServerWideExternalReplication ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <StudioTrans
                        ns="editServerWideExternalReplication"
                        i18nKey="about.serverWideTask"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xs">
                        <li>{t("about.excludeItem")}</li>
                        <li>{t("about.configurationsItem")}</li>
                    </ul>
                </div>
                <div className="margin-top-sm">
                    <StudioTrans
                        ns="editServerWideExternalReplication"
                        i18nKey="about.connectionString"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xs">
                        <li>{t("about.discoveryUrlsItem")}</li>
                        <li>{t("about.databaseNameItem")}</li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={featureAvailability}
                isUnlimited={hasServerWideExternalReplication}
            />
        </AboutViewFloating>
    );
}
