import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function DocumentSchemaPlaygroundAboutView() {
    const t = useStudioTranslation("documentSchema");
    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.schemaValidation"),
            featureIcon: "document-schema",
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
                value: hasSchemaValidation,
            },
        ],
    });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper icon="about" color="info">
                <div>
                    <ul>
                        <li>{t("playground.about.test")}</li>
                        <li className="mt-2">{t("playground.about.safe")}</li>
                        <li className="mt-2">{t("playground.about.steps")}</li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasSchemaValidation} data={featureAvailability} />
        </AboutViewAnchored>
    );
}
