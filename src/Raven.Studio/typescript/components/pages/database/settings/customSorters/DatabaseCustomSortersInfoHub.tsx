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

interface DatabaseCustomSortersInfoHubProps {
    databaseSortersCount: number;
}

export function DatabaseCustomSortersInfoHub({ databaseSortersCount }: DatabaseCustomSortersInfoHubProps) {
    const t = useStudioTranslation("customSorters");
    const commonT = useStudioTranslation("common");
    const customSortersDocsLink = useRavenLink({ hash: "XI6BMT" });
    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerDatabase"));
    const numberOfCustomSortersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfCustomSortersInCluster;
    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));

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
                value: hasServerWideCustomSorters,
            },
        ],
    });

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseSortersCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomSortersInCluster, licenseClusterLimit);

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="customSorters"
                        i18nKey="about.intro"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <div>
                    <StudioTrans ns="customSorters" i18nKey="about.inThisView" components={{ strong: <strong /> }} />
                    <ul className="margin-top-xxs">
                        <li>{t("about.databaseScopeItem")}</li>
                        <li>{t("about.serverWideScopeItem")}</li>
                        <li>{t("about.testItem")}</li>
                        <li>{t("about.coraxItem")}</li>
                    </ul>
                </div>
                <div>
                    <StudioTrans ns="customSorters" i18nKey="about.provideCode" components={{ code: <code /> }} />
                    <ul className="margin-top-xxs">
                        <li>{t("about.nameItem")}</li>
                        <li>
                            <StudioTrans
                                ns="customSorters"
                                i18nKey="about.inheritItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="customSorters"
                                i18nKey="about.compilableItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={customSortersDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={
                    databaseLimitReachStatus === "notReached" &&
                    clusterLimitReachStatus === "notReached" &&
                    hasServerWideCustomSorters
                }
                isOpenedByDefault={false}
                data={featureAvailability}
            />
        </AboutViewAnchored>
    );
}
