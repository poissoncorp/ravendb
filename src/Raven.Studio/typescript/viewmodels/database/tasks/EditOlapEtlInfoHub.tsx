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

export function EditOlapEtlInfoHub() {
    const t = useStudioTranslation("editOlapEtlTask");
    const commonT = useStudioTranslation("common");
    const hasOlapEtl = useAppSelector(licenseSelectors.statusValue("HasOlapEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasOlapEtl,
            },
        ],
    });

    const olapEtlDocsLink = useRavenLink({ hash: "LYZL56" });

    return (
        <AboutViewFloating defaultOpen={hasOlapEtl ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editOlapEtlTask" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans ns="editOlapEtlTask" i18nKey="about.documentsOnly" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">{t("about.transformationScripts")}</li>
                    <li className="margin-top-xxs">{t("about.scriptCollections")}</li>
                    <li className="margin-top-xxs">{t("about.scriptExecution")}</li>
                    <li className="margin-top-xxs">{t("about.testScripts")}</li>
                </ul>
                <hr />
                <div>
                    {t("about.definitionIncludes")}
                    <ul>
                        <li>{t("about.definitionFrequency")}</li>
                        <li>{t("about.definitionConnectionString")}</li>
                        <li>{t("about.definitionScripts")}</li>
                        <li>{t("about.definitionIdColumn")}</li>
                        <li>{t("about.definitionResponsibleNode")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={olapEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasOlapEtl} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "OLAP ETL",
        featureIcon: "olap-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
        quill: { value: true },
    },
];
