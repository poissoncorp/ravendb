import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { Icon } from "components/common/Icon";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ServerWideCustomAnalyzersInfoHub() {
    const t = useStudioTranslation("serverWideAnalyzers");
    const commonT = useStudioTranslation("common");
    const customAnalyzersDocsLink = useRavenLink({ hash: "VWCQPI" });

    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));
    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.serverWide"),
            featureIcon: "server-wide-custom-analyzers",
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
                value: hasServerWideCustomAnalyzers,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasServerWideCustomAnalyzers ? null : "licensing"}>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="serverWideAnalyzers"
                        i18nKey="about.analyzers"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <p>
                    <StudioTrans
                        ns="serverWideAnalyzers"
                        i18nKey="about.inThisView"
                        components={{ strong: <strong /> }}
                    />
                </p>
                <div>
                    <StudioTrans ns="serverWideAnalyzers" i18nKey="about.provideCode" components={{ code: <code /> }} />
                    <ul>
                        <li>{t("about.nameItem")}</li>
                        <li>
                            <StudioTrans
                                ns="serverWideAnalyzers"
                                i18nKey="about.inheritItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="serverWideAnalyzers"
                                i18nKey="about.compilableItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={customAnalyzersDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasServerWideCustomAnalyzers} data={featureAvailability} />
        </AboutViewAnchored>
    );
}
