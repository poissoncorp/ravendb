import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import React from "react";
import RichAlert from "components/common/RichAlert";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface IntegrationsAlertsProps {
    isLicenseUpgradeRequired: boolean;
    isPostgreSqlSupportEnabled: boolean;
}

export default function IntegrationsAlerts(props: IntegrationsAlertsProps) {
    const t = useStudioTranslation("integrations");
    const { isLicenseUpgradeRequired, isPostgreSqlSupportEnabled } = props;

    const postgreSqlDocsLink = useRavenLink({ hash: "HDTCH7" });
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    if (isLicenseUpgradeRequired) {
        return (
            <RichAlert variant="warning">
                <span>{t("alerts.licenseUpgradeRequired")}</span>

                <div className="mt-1">
                    <a href={buyLink} target="_blank" className="btn btn-primary rounded-pill">
                        {t("alerts.licensingOptions")} <Icon icon="newtab" margin="ms-1" />
                    </a>
                </div>
            </RichAlert>
        );
    }

    if (!isLicenseUpgradeRequired && !isPostgreSqlSupportEnabled) {
        return (
            <RichAlert variant="warning" className="mt-3">
                <StudioTrans
                    ns="integrations"
                    i18nKey="alerts.postgreSqlNotEnabled"
                    components={{ code: <code />, a: <a href={postgreSqlDocsLink} /> }}
                />
            </RichAlert>
        );
    }

    return null;
}
