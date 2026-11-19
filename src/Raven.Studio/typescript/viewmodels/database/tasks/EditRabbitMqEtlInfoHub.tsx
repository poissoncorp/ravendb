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

export function EditRabbitMqEtlInfoHub() {
    const t = useStudioTranslation("editRabbitMqEtlTask");
    const commonT = useStudioTranslation("common");
    const hasQueueEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.rabbitMqEtl"),
            featureIcon: "rabbitmq-etl",
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
                value: hasQueueEtl,
            },
        ],
    });

    const rabbitMqEtlDocsLink = useRavenLink({ hash: "KFKQM7" });

    return (
        <AboutViewFloating defaultOpen={hasQueueEtl ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editRabbitMqEtlTask" i18nKey="about.task" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="editRabbitMqEtlTask"
                            i18nKey="about.documentsOnlyItem"
                            components={{ br: <br /> }}
                        />
                    </li>
                    <li className="margin-top-xxs">{t("about.filterItem")}</li>
                    <li className="margin-top-xxs">{t("about.collectionsItem")}</li>
                    <li className="margin-top-xxs">{t("about.executionItem")}</li>
                    <li className="margin-top-xxs">{t("about.testItem")}</li>
                </ul>
                <hr />
                <div>
                    {t("about.taskDefinition")}
                    <ul>
                        <li>{t("about.connectionStringItem")}</li>
                        <li>{t("about.scriptsItem")}</li>
                        <li>{t("about.deleteItem")}</li>
                        <li>{t("about.skipCreationItem")}</li>
                        <li>{t("about.responsibleNodeItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={rabbitMqEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasQueueEtl} data={featureAvailability} />
        </AboutViewFloating>
    );
}
