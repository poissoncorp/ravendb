import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditRavenEtlInfoHub() {
    const t = useStudioTranslation("editRavenEtlTask");
    const commonT = useStudioTranslation("common");
    const hasRavenEtl = useAppSelector(licenseSelectors.statusValue("HasRavenEtl"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.ravenEtl"),
            featureIcon: "ravendb-etl",
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
                value: hasRavenEtl,
            },
        ],
    });

    const ravenDbEtlDocsLink = useRavenLink({ hash: "GFSWLI" });

    return (
        <AboutViewFloating defaultOpen={hasRavenEtl ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editRavenEtlTask" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>{t("about.transformationScripts")}</li>
                    <li className="margin-top-xxs">
                        <StudioTrans ns="editRavenEtlTask" i18nKey="about.customLogic" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">{t("about.scriptCollections")}</li>
                    <li className="margin-top-xxs">{t("about.scriptExecution")}</li>
                    <li className="margin-top-xxs">{t("about.testScripts")}</li>
                </ul>
                <hr />
                <div>
                    {t("about.definitionIncludes")}
                    <ul>
                        <li>{t("about.definitionScripts")}</li>
                        <li>{t("about.definitionConnectionString")}</li>
                        <li>{t("about.definitionResponsibleNode")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={ravenDbEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasRavenEtl} data={featureAvailability} />
        </AboutViewFloating>
    );
}
