import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import { useRavenLink } from "hooks/useRavenLink";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function TimeSeriesInfoHub() {
    const t = useStudioTranslation("timeSeriesSettings");
    const commonT = useStudioTranslation("common");
    const hasTimeSeriesRollupsAndRetention = useAppSelector(
        licenseSelectors.statusValue("HasTimeSeriesRollupsAndRetention")
    );
    const timeSeriesDocsLink = useRavenLink({ hash: "LNOMKT" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.rollupsAndRetention"),
            featureIcon: "timeseries-settings",
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
                value: hasTimeSeriesRollupsAndRetention,
            },
        ],
    });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="timeSeriesSettings" i18nKey="about.intro" components={{ strong: <strong /> }} />
                </p>
                <div>
                    {t("about.configurablePerCollection")}
                    <ul>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="timeSeriesSettings"
                                i18nKey="about.rollupPolicies"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="timeSeriesSettings"
                                i18nKey="about.retentionPeriods"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="timeSeriesSettings"
                                i18nKey="about.namedValues"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div>
                    {t("about.applying")}
                    <ul>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="timeSeriesSettings"
                                i18nKey="about.policyCheckFrequency"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">{t("about.enableOrDisable")}</li>
                        <li className="margin-top-xxs">{t("about.appliesToCollection")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={timeSeriesDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasTimeSeriesRollupsAndRetention}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}
