import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditSubscriptionTaskInfoHub() {
    const t = useStudioTranslation("editSubscriptionTask");
    const commonT = useStudioTranslation("common");
    const hasConcurrentDataSubscriptions = useAppSelector(
        licenseSelectors.statusValue("HasConcurrentDataSubscriptions")
    );
    const hasRevisionsInSubscriptions = useAppSelector(licenseSelectors.statusValue("HasRevisionsInSubscriptions"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.concurrentDataSubscriptions"),
            featureIcon: "data-subscriptions",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.subscriptionsRevisions"),
            featureIcon: "revisions",
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
                value: hasConcurrentDataSubscriptions,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasRevisionsInSubscriptions,
            },
        ],
    });

    const subscriptionTasksDocsLink = useRavenLink({ hash: "I5TMCK" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="editSubscriptionTask"
                        i18nKey="about.defineQuery"
                        components={{ strong: <strong /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="editSubscriptionTask"
                        i18nKey="about.matchingDocuments"
                        components={{ strong: <strong /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="editSubscriptionTask"
                        i18nKey="about.ongoingProcess"
                        components={{ strong: <strong /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="editSubscriptionTask"
                        i18nKey="about.batches"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="editSubscriptionTask"
                            i18nKey="about.startingPoint"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                    <li className="margin-top-xxs">
                        <StudioTrans
                            ns="editSubscriptionTask"
                            i18nKey="about.testQuery"
                            components={{ strong: <strong /> }}
                        />
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={subscriptionTasksDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={featureAvailability}
                isUnlimited={hasConcurrentDataSubscriptions && hasRevisionsInSubscriptions}
                isOpenedByDefault={!hasConcurrentDataSubscriptions && !hasRevisionsInSubscriptions}
            />
        </AboutViewFloating>
    );
}
