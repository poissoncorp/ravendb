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
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditIndexInfoHub() {
    const t = useStudioTranslation("editIndex");
    const commonT = useStudioTranslation("common");
    const hasAdditionalAssembliesFromNuGet = useAppSelector(
        licenseSelectors.statusValue("HasAdditionalAssembliesFromNuGet")
    );

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.additionalAssembliesFromNuGet"),
            featureIcon: "additional-assemblies",
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
                value: hasAdditionalAssembliesFromNuGet,
            },
        ],
    });
    const indexViewDocsLink = useRavenLink({ hash: "XZXJP2" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editIndex" i18nKey="about.intro" components={{ br: <br /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans ns="editIndex" i18nKey="about.indexTypes" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">
                        <StudioTrans ns="editIndex" i18nKey="about.deploymentMode" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">
                        <StudioTrans ns="editIndex" i18nKey="about.fields" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">
                        <StudioTrans ns="editIndex" i18nKey="about.configuration" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">{t("about.additionalSources")}</li>
                    <li className="margin-top-xxs">{t("about.test")}</li>
                    <li className="margin-top-xxs">
                        <StudioTrans ns="editIndex" i18nKey="about.history" components={{ br: <br /> }} />
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={indexViewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasAdditionalAssembliesFromNuGet}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}
