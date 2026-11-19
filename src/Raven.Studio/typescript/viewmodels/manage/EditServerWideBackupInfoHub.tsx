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
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditServerWideBackupInfoHub() {
    const t = useStudioTranslation("editServerWideBackup");
    const commonT = useStudioTranslation("common");
    const hasServerWideBackups = useAppSelector(licenseSelectors.statusValue("HasServerWideBackups"));
    const hasSnapshotBackups = useAppSelector(licenseSelectors.statusValue("HasSnapshotBackups"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = [
        {
            featureName: t("featureAvailability.serverWideBackups"),
            featureIcon: "server-wide-backup",
            community: { value: false },
            professional: { value: true },
            enterprise: { value: true },
            quill: { value: true },
        },
        {
            featureName: t("featureAvailability.snapshotBackups"),
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
                value: hasServerWideBackups,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: hasSnapshotBackups,
            },
        ],
    });

    const backupTasksDocsLink = useRavenLink({ hash: "SXSM33" });

    return (
        <AboutViewFloating defaultOpen={hasServerWideBackups ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    <StudioTrans
                        ns="editServerWideBackup"
                        i18nKey="about.serverWideTask"
                        components={{ strong: <strong /> }}
                    />
                    <ul className="margin-top-xxs">
                        <li>{t("about.excludeItem")}</li>
                        <li>{t("about.configurationsItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("about.configurationOptions")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.backupTypeItem")}</li>
                        <li>{t("about.backupKindsItem")}</li>
                        <li>{t("about.retentionItem")}</li>
                        <li>{t("about.destinationsItem")}</li>
                        <li>{t("about.encryptionItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("about.inAddition")}
                    <ul className="margin-top-xxs">
                        <li>{t("about.responsibleNodeItem")}</li>
                        <li>{t("about.disablingItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={backupTasksDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("about.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                data={featureAvailability}
                isUnlimited={hasServerWideBackups && hasSnapshotBackups}
                isOpenedByDefault={!hasServerWideBackups && !hasSnapshotBackups}
            />
        </AboutViewFloating>
    );
}
