import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditAmazonSqsEtlInfoHub() {
    const t = useStudioTranslation("editAmazonSqsEtlTask");
    const hasQueueEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.amazonSqsEtl"),
            featureIcon: "amazon-sqs-etl",
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

    return (
        <AboutViewFloating defaultOpen={hasQueueEtl ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans ns="editAmazonSqsEtlTask" i18nKey="about.task" components={{ strong: <strong /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="editAmazonSqsEtlTask"
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
                        <li>{t("about.responsibleNodeItem")}</li>
                    </ul>
                </div>
                <hr />
                {/* TODO
                <div className="small-label mb-2">useful links</div>    
                <a href={amazonSqsEtlDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Amazon SQS ETL
                </a>
                 */}
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasQueueEtl} data={featureAvailability} />
        </AboutViewFloating>
    );
}
