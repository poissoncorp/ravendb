import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "hooks/useRavenLink";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function CertificatesInfoHub() {
    const t = useStudioTranslation("certificates");
    const commonT = useStudioTranslation("common");
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));
    const certificatesDocsLink = useRavenLink({ hash: "S3G2T1" });

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.readOnlyCertificates"),
            featureIcon: "access-read",
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
                value: hasReadOnlyCertificates,
            },
        ],
    });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>{t("about.intro")}</p>
                <div>
                    <StudioTrans ns="certificates" i18nKey="about.serverHeading" components={{ strong: <strong /> }} />
                    <ul>
                        <li className="margin-top-xxs">{t("about.serverExportItem")}</li>
                        <li className="margin-top-xxs">{t("about.serverReplaceItem")}</li>
                    </ul>
                </div>
                <div>
                    <strong>{t("about.clientHeading")}</strong>
                    <ul>
                        <li className="margin-top-xxs">{t("about.clientGenerateItem")}</li>
                        <li className="margin-top-xxs">
                            <StudioTrans
                                ns="certificates"
                                i18nKey="about.clientPermissionsItem"
                                components={{ br: <br /> }}
                            />
                        </li>
                        <li className="margin-top-xxs">{t("about.clientUploadItem")}</li>
                        <li className="margin-top-xxs">{t("about.privateKeyItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={certificatesDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper isUnlimited={hasReadOnlyCertificates} data={featureAvailability} />
        </AboutViewFloating>
    );
}
