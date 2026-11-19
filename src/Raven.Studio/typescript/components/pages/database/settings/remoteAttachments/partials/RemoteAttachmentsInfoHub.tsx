import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function RemoteAttachmentsInfoHub() {
    const t = useStudioTranslation("remoteAttachments");
    const hasRemoteAttachments = useAppSelector(licenseSelectors.statusValue("HasRemoteAttachments"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.remoteAttachments"),
            featureIcon: "remote-attachment",
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
                value: hasRemoteAttachments,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasRemoteAttachments ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <p>
                        <StudioTrans ns="remoteAttachments" i18nKey="about.intro" components={{ strong: <strong /> }} />
                    </p>
                    <p>
                        <StudioTrans
                            ns="remoteAttachments"
                            i18nKey="about.whenEnabled"
                            components={{ strong: <strong /> }}
                        />
                    </p>
                    <ul>
                        <li>{t("about.backgroundTaskItem")}</li>
                        <li className="mt-1">{t("about.nonBlockingItem")}</li>
                    </ul>
                    <p>{t("about.inThisView")}</p>
                    <ul>
                        <li>{t("about.enableItem")}</li>
                        <li className="mt-1">
                            <StudioTrans
                                ns="remoteAttachments"
                                i18nKey="about.destinationsItem"
                                components={{ strong: <strong /> }}
                            />
                        </li>
                        <li className="mt-1">
                            {t("about.fineTuneItem")}
                            <ul>
                                <li>{t("about.intervalItem")}</li>
                                <li>{t("about.maxItemsItem")}</li>
                                <li>{t("about.concurrentUploadsItem")}</li>
                            </ul>
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasRemoteAttachments} data={featureAvailability} />
        </AboutViewAnchored>
    );
}
