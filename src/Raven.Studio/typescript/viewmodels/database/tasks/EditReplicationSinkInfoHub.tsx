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

export function EditReplicationSinkInfoHub() {
    const t = useStudioTranslation("editReplicationSinkTask");
    const commonT = useStudioTranslation("common");
    const hasPullReplicationAsSink = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsSink"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.replicationSink"),
            featureIcon: "pull-replication-agent",
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
                value: hasPullReplicationAsSink,
            },
        ],
    });

    const replicationSinkDocsLink = useRavenLink({ hash: "MMUKGD" });

    return (
        <AboutViewFloating defaultOpen={hasPullReplicationAsSink ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <StudioTrans
                        ns="editReplicationSinkTask"
                        i18nKey="about.definition"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xxs">
                        <li>{t("about.initiatedBySink")}</li>
                        <li>{t("about.multipleSinks")}</li>
                        <li>{t("about.bidirectional")}</li>
                        <li>{t("about.replicatedData")}</li>
                    </ul>
                </div>
                <hr />
                <div>
                    {t("about.secureServer")}
                    <ul>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="editReplicationSinkTask"
                                i18nKey="about.replicationAccess"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="editReplicationSinkTask"
                                i18nKey="about.filteredReplication"
                                components={{ strong: <strong />, br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="editReplicationSinkTask"
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
                        <li>{t("about.connectionString")}</li>
                        <li>{t("about.direction")}</li>
                        <li>{t("about.accessDefinition")}</li>
                        <li>{t("about.responsibleNode")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={replicationSinkDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasPullReplicationAsSink} data={featureAvailability} />
        </AboutViewFloating>
    );
}
