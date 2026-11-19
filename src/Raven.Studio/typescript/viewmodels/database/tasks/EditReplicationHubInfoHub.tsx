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

export function EditReplicationHubInfoHub() {
    const t = useStudioTranslation("editReplicationHubTask");
    const commonT = useStudioTranslation("common");
    const hasPullReplicationAsHub = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsHub"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.replicationHub"),
            featureIcon: "pull-replication-hub",
            community: { value: false },
            professional: { value: false },
            enterprise: { value: true },
            quill: { value: true },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasPullReplicationAsHub,
            },
        ],
    });

    const replicationHubDocsLink = useRavenLink({ hash: "NIH5LN" });

    return (
        <AboutViewFloating defaultOpen={hasPullReplicationAsHub ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <StudioTrans
                        ns="editReplicationHubTask"
                        i18nKey="about.definition"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xxs">
                        <li>{t("about.servesManySinks")}</li>
                        <li>{t("about.initiatedBySink")}</li>
                        <li>{t("about.replicatedData")}</li>
                    </ul>
                </div>
                <hr />
                <div>
                    {t("about.secureServer")}
                    <ul>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="editReplicationHubTask"
                                i18nKey="about.replicationAccess"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="editReplicationHubTask"
                                i18nKey="about.filteredReplication"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="editReplicationHubTask"
                                i18nKey="about.sinkToHub"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div>
                    {t("about.taskDefinition")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.direction")}</li>
                        <li>{t("about.accessDefinitions")}</li>
                        <li>{t("about.deletions")}</li>
                        <li>{t("about.delay")}</li>
                        <li>{t("about.responsibleNode")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={replicationHubDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasPullReplicationAsHub} data={featureAvailability} />
        </AboutViewFloating>
    );
}
