import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditExternalReplicationInfoHub() {
    const t = useStudioTranslation("editExternalReplicationTask");
    const commonT = useStudioTranslation("common");
    const hasExternalReplication = useAppSelector(licenseSelectors.statusValue("HasExternalReplication"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.externalReplication"),
            featureIcon: "external-replication",
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
                value: hasExternalReplication,
            },
        ],
    });

    const externalReplicationDocsLink = useRavenLink({ hash: "MZOBO3" });

    return (
        <AboutViewFloating defaultOpen={hasExternalReplication ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <StudioTrans
                        ns="editExternalReplicationTask"
                        i18nKey="about.task"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xxs">
                        <li>{t("about.failoverItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("about.whatIsReplicated")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.documentsItem")}</li>
                        <li>{t("about.clusterItemsItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("about.taskDefinition")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.connectionStringItem")}</li>
                        <li>{t("about.delayItem")}</li>
                        <li>{t("about.responsibleNodeItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={externalReplicationDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasExternalReplication} data={featureAvailability} />
        </AboutViewFloating>
    );
}
