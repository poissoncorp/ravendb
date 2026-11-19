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

export default function ServerWideCustomSortersInfoHub() {
    const t = useStudioTranslation("serverWideSorters");
    const commonT = useStudioTranslation("common");
    const customSortersDocsLink = useRavenLink({ hash: "XI6BMT" });

    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));
    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.serverWide"),
            featureIcon: "server-wide-custom-sorters",
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
                value: hasServerWideCustomSorters,
            },
        ],
    });

    return (
        <AboutViewAnchored defaultOpen={hasServerWideCustomSorters ? null : "licensing"}>
            <AccordionItemWrapper targetId="1" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="serverWideSorters"
                        i18nKey="about.sorters"
                        components={{ strong: <strong />, br: <br /> }}
                    />
                </p>
                <div>
                    <StudioTrans
                        ns="serverWideSorters"
                        i18nKey="about.inThisView"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xxs">
                        <li>{t("about.allDatabasesItem")}</li>
                        <li>{t("about.coraxItem")}</li>
                    </ul>
                </div>
                <div>
                    <StudioTrans ns="serverWideSorters" i18nKey="about.provideCode" components={{ code: <code /> }} />
                    <ul className="margin-top-xxs">
                        <li>{t("about.nameItem")}</li>
                        <li>
                            <StudioTrans
                                ns="serverWideSorters"
                                i18nKey="about.inheritItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                        <li>
                            <StudioTrans
                                ns="serverWideSorters"
                                i18nKey="about.compilableItem"
                                components={{ code: <code /> }}
                            />
                        </li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={customSortersDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasServerWideCustomSorters} data={featureAvailability} />
        </AboutViewAnchored>
    );
}
