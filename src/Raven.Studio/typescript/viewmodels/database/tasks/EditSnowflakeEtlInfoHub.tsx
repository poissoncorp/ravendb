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

export function EditSnowflakeEtlInfoHub() {
    const t = useStudioTranslation("editSnowflakeEtlTask");
    const commonT = useStudioTranslation("common");
    const hasSnowflakeEtl = useAppSelector(licenseSelectors.statusValue("HasSnowflakeEtl"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasSnowflakeEtl,
            },
        ],
    });

    const snowflakeEtlDocsLink = useRavenLink({ hash: "TODO" }); //TODO: HASH

    return (
        <AboutViewFloating defaultOpen={hasSnowflakeEtl ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editSnowflakeEtlTask" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="editSnowflakeEtlTask"
                            i18nKey="about.extractedData"
                            components={{ br: <br /> }}
                        />
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
                        <li>
                            <StudioTrans
                                ns="editSnowflakeEtlTask"
                                i18nKey="about.definitionTables"
                                components={{ br: <br /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="editSnowflakeEtlTask"
                                i18nKey="about.definitionConnection"
                                components={{ br: <br />, i: <i /> }}
                            />
                        </li>
                        <li>{t("about.definitionResponsibleNode")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={snowflakeEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasSnowflakeEtl} data={featureAvailability} />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Snowflake ETL",
        featureIcon: "snowflake-etl",
        community: { value: false },
        professional: { value: false },
        enterprise: { value: true },
        quill: { value: true },
    },
];
