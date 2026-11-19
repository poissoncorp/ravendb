import { ConditionalPopover } from "components/common/ConditionalPopover";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import React from "react";
import Button from "react-bootstrap/Button";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface IntegrationsAddNewButtonProps {
    isLicenseUpgradeRequired: boolean;
    addNewUser: () => void;
}

export default function IntegrationsAddNewButton(props: IntegrationsAddNewButtonProps) {
    const t = useStudioTranslation("integrations");
    const { isLicenseUpgradeRequired, addNewUser } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: isLicenseUpgradeRequired,
                    message: <LicenseRestrictedMessage>{t("addNew.licenseRestricted")}</LicenseRestrictedMessage>,
                },
                { isActive: !hasDatabaseAdminAccess, message: t("addNew.noPermissions") },
            ]}
        >
            <Button
                variant="info"
                size="sm"
                className="rounded-pill"
                title={t("addNew.title")}
                onClick={addNewUser}
                disabled={isLicenseUpgradeRequired || !hasDatabaseAdminAccess}
            >
                <Icon icon="plus" />
                {t("addNew.button")}
            </Button>
        </ConditionalPopover>
    );
}
