import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import React from "react";
import { useRavenLink } from "hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { getLicenseLimitReachStatus, useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DatabaseCustomAnalyzersInfoHubProps {
    databaseAnalyzersCount: number;
}

export function DatabaseCustomAnalyzersInfoHub({ databaseAnalyzersCount }: DatabaseCustomAnalyzersInfoHubProps) {
    const t = useStudioTranslation("databaseCustomAnalyzers");
    const commonT = useStudioTranslation("common");
    const customAnalyzersDocsLink = useRavenLink({ hash: "VWCQPI" });
    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerDatabase"));
    const numberOfCustomAnalyzersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfAnalyzersInCluster;
    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.limitPerDatabase"),
            community: { value: 1 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
        {
            featureName: t("featureAvailability.limitPerCluster"),
            community: { value: 5 },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
        {
            featureName: t("featureAvailability.serverWide"),
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
                value: licenseDatabaseLimit,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: licenseClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: hasServerWideCustomAnalyzers,
            },
        ],
    });

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseAnalyzersCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomAnalyzersInCluster, licenseClusterLimit);

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="databaseCustomAnalyzers"
                        i18nKey="about.analyzers"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <div>
                    <StudioTrans
                        ns="databaseCustomAnalyzers"
                        i18nKey="about.inThisView"
                        components={{ strong: <strong /> }}
                    />
                    <ul>
                        <li>{t("about.databaseScopeItem")}</li>
                        <li>{t("about.serverWideScopeItem")}</li>
                    </ul>
                </div>
                <div>
                    <StudioTrans
                        ns="databaseCustomAnalyzers"
                        i18nKey="about.provideCode"
                        components={{ code: <code /> }}
                    />
                    <ul>
                        <li>{t("about.nameItem")}</li>
                        <li>
                            <StudioTrans
                                ns="databaseCustomAnalyzers"
                                i18nKey="about.inheritItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="databaseCustomAnalyzers"
                                i18nKey="about.compilableItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={customAnalyzersDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={
                    databaseLimitReachStatus === "notReached" &&
                    clusterLimitReachStatus === "notReached" &&
                    hasServerWideCustomAnalyzers
                }
                isOpenedByDefault={false}
                data={featureAvailability}
            />
        </AboutViewAnchored>
    );
}
