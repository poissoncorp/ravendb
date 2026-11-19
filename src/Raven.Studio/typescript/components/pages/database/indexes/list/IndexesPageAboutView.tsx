import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { Icon } from "components/common/Icon";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface IndexesPageAboutViewProps {
    isUnlimited: boolean;
}

export default function IndexesPageAboutView({ isUnlimited }: IndexesPageAboutViewProps) {
    const t = useStudioTranslation("indexesList");
    const commonT = useStudioTranslation("common");
    const overviewDocsLink = useRavenLink({ hash: "8VWNHJ" });
    const listDocsLink = useRavenLink({ hash: "7HOOEA" });

    const autoClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerCluster"));
    const staticClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerCluster"));
    const autoDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerDatabase"));
    const staticDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerDatabase"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.autoPerCluster"),
            community: { value: Infinity },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
        {
            featureName: t("featureAvailability.staticPerCluster"),
            community: { value: Infinity },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
        {
            featureName: t("featureAvailability.autoPerDatabase"),
            community: { value: Infinity },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
        {
            featureName: t("featureAvailability.staticPerDatabase"),
            community: { value: Infinity },
            professional: { value: Infinity },
            enterprise: { value: Infinity },
            quill: { value: Infinity },
        },
    ];

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: autoClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: staticClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: autoDatabaseLimit,
            },
            {
                featureName: defaultFeatureAvailability[3].featureName,
                value: staticDatabaseLimit,
            },
        ],
    });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <p>
                    <StudioTrans ns="indexesList" i18nKey="about.intro" components={{ br: <br /> }} />
                </p>
                <ul>
                    <li>
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="about.detailedInfo"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                    <li className="margin-top-xs">
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="about.variousActions"
                            components={{ strong: <strong />, br: <br /> }}
                        />
                    </li>
                </ul>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={overviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.overviewDocsLink")}
                </a>
                <br />
                <a href={listDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.listDocsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isUnlimited}
                isOpenedByDefault={false}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}
