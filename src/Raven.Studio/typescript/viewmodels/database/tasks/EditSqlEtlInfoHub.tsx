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

export function EditSqlEtlInfoHub() {
    const t = useStudioTranslation("editSqlEtlTask");
    const commonT = useStudioTranslation("common");
    const hasSqlEtl = useAppSelector(licenseSelectors.statusValue("HasSqlEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasSqlEtl,
            },
        ],
    });

    const sqlEtlDocsLink = useRavenLink({ hash: "7J6SEO" });

    return (
        <AboutViewFloating defaultOpen={hasSqlEtl ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editSqlEtlTask" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans ns="editSqlEtlTask" i18nKey="about.extractedData" components={{ br: <br /> }} />
                    </li>
                    <li className="margin-top-xxs">{t("about.transformationScripts")}</li>
                    <li className="margin-top-xxs">{t("about.scriptCollections")}</li>
                    <li className="margin-top-xxs">{t("about.scriptExecution")}</li>
                    <li className="margin-top-xxs">{t("about.testScripts")}</li>
                </ul>
                <hr />
                <div>
                    {t("about.taskDefinition")}
                    <ul>
                        <li>{t("about.definitionScripts")}</li>
                        <li>{t("about.definitionTables")}</li>
                        <li>{t("about.definitionConnection")}</li>
                        <li>{t("about.definitionResponsibleNode")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={sqlEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasSqlEtl} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "SQL ETL",
        featureIcon: "sql-etl",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
        quill: { value: true },
    },
];
