import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function DocumentSchemaAboutView() {
    const jsonSchemaLink = "https://json-schema.org";
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
                        <li>
                            <StudioTrans
                                ns="documentSchema"
                                i18nKey="about.defineSchema"
                                components={{ strong: <strong />, a: <a href={jsonSchemaLink} target="_blank" /> }}
                            />
                        </li>
                        <li className="mt-2">
                            <StudioTrans
                                ns="documentSchema"
                                i18nKey="about.enforced"
                                components={{ strong: <strong /> }}
                            />
                            <ul>
                                <li>
                                    <StudioTrans
                                        ns="documentSchema"
                                        i18nKey="about.enforcedSession"
                                        components={{ i: <i /> }}
                                    />
                                </li>
                                <li>
                                    <StudioTrans
                                        ns="documentSchema"
                                        i18nKey="about.enforcedPatch"
                                        components={{ i: <i /> }}
                                    />
                                </li>
                                <li>{t("about.enforcedImport")}</li>
                                <li>{t("about.enforcedEtl")}</li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            <StudioTrans
                                ns="documentSchema"
                                i18nKey="about.bypassed"
                                components={{ strong: <strong /> }}
                            />
                            <ul>
                                <li>{t("about.bypassedInternalReplication")}</li>
                                <li>{t("about.bypassedExternalReplication")}</li>
                                <li>{t("about.bypassedRestore")}</li>
                            </ul>
                        </li>
                        <li className="mt-2">
                            <StudioTrans
                                ns="documentSchema"
                                i18nKey="about.keywords"
                                components={{ strong: <strong /> }}
                            />
                            <ul>
                                <li>
                                    <StudioTrans
                                        ns="documentSchema"
                                        i18nKey="about.keywordsUnevaluated"
                                        components={{ code: <code /> }}
                                    />
                                </li>
                                <li>{t("about.keywordsRecursive")}</li>
                            </ul>
                        </li>
                        <li className="mt-2">{t("about.enableDisable")}</li>
                        <li className="mt-2">{t("about.revisionsNote")}</li>
                        <li className="mt-2">
                            <StudioTrans
                                ns="documentSchema"
                                i18nKey="about.playground"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasSchemaValidation} data={featureAvailability} />
        </AboutViewAnchored>
    );
}
