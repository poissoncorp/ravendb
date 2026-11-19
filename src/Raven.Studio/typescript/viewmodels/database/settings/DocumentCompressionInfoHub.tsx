import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { useRavenLink } from "hooks/useRavenLink";
import { useEnterpriseLicenseAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function DocumentCompressionInfoHub() {
    const t = useStudioTranslation("documentCompressionInfoHub");
    const commonT = useStudioTranslation("common");
    const isFeatureInLicense = useAppSelector(licenseSelectors.statusValue("HasDocumentsCompression"));
    const featureAvailability = useEnterpriseLicenseAvailability(isFeatureInLicense);

    const compressionDocsLink = useRavenLink({ hash: "E2WX16" });

    return (
        <AboutViewFloating defaultOpen={isFeatureInLicense ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>{t("about.intro")}</p>
                <div>
                    {t("about.canBeSetFor")}
                    <ul>
                        <li>{t("about.documentsItem")}</li>
                        <li>{t("about.revisionsItem")}</li>
                    </ul>
                </div>
                <p>{t("about.notApplied")}</p>
                <div>
                    {t("about.triggeredWhen")}
                    <ul>
                        <li>{t("about.storingItem")}</li>
                        <li>{t("about.modifyingItem")}</li>
                        <li>{t("about.compactItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={compressionDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={isFeatureInLicense} data={featureAvailability} />
        </AboutViewFloating>
    );
}
