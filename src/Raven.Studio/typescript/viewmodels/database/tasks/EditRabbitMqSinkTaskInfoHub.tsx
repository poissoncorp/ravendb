import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditRabbitMqSinkTaskInfoHub() {
    const t = useStudioTranslation("editRabbitMqSinkTask");
    const hasQueueSink = useAppSelector(licenseSelectors.statusValue("HasQueueSink"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.rabbitMqSink"),
            featureIcon: "rabbitmq-sink",
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
                value: hasQueueSink,
            },
        ],
    });

    return (
        <AboutViewFloating defaultOpen={hasQueueSink ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editRabbitMqSinkTask" i18nKey="about.task" components={{ strong: <strong /> }} />
                </p>
                <p>
                    <StudioTrans
                        ns="editRabbitMqSinkTask"
                        i18nKey="about.scripts"
                        components={{ strong: <strong /> }}
                    />
                </p>
                <ul>
                    <li className="margin-top-sm">
                        <StudioTrans
                            ns="editRabbitMqSinkTask"
                            i18nKey="about.taskDefinitionItem"
                            components={{ br: <br /> }}
                        />
                    </li>
                    <li className="margin-top-xxs">
                        <StudioTrans
                            ns="editRabbitMqSinkTask"
                            i18nKey="about.scriptDefinitionItem"
                            components={{ br: <br /> }}
                        />
                    </li>
                    <li className="margin-top-xxs">{t("about.jsonItem")}</li>
                </ul>
                <p>
                    <StudioTrans ns="editRabbitMqSinkTask" i18nKey="about.notIdle" components={{ br: <br /> }} />
                </p>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasQueueSink} data={featureAvailability} />
        </AboutViewFloating>
    );
}
