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

export function EditManualBackupTaskInfoHub() {
    const t = useStudioTranslation("editPeriodicBackupTask");
    const commonT = useStudioTranslation("common");
    const hasEncryptedBackups = useAppSelector(licenseSelectors.statusValue("HasEncryptedBackups"));
    const hasCloudBackups = useAppSelector(licenseSelectors.statusValue("HasCloudBackups"));
    const hasSnapshotBackups = useAppSelector(licenseSelectors.statusValue("HasSnapshotBackups"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.manual.encryption"),
            featureIcon: "encryption",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.manual.remoteDestinations"),
            featureIcon: "cloud",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.manual.snapshotBackups"),
            featureIcon: "snapshot-backup",
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
                value: hasEncryptedBackups,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasCloudBackups,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: hasSnapshotBackups,
            },
        ],
    });

    const backupsDocsLink = useRavenLink({ hash: "GMBYOH" });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    {t("about.manual.intro")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.manual.adHocItem")}</li>
                        <li>{t("about.manual.noRetentionItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("about.configurationOptions")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.backupTypeItem")}</li>
                        <li>{t("about.manual.destinationsItem")}</li>
                        <li>{t("about.encryptionItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={backupsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasEncryptedBackups && hasCloudBackups && hasSnapshotBackups}
                data={featureAvailability}
                isOpenedByDefault={!hasEncryptedBackups && !hasCloudBackups && !hasSnapshotBackups}
            />
        </AboutViewFloating>
    );
}
