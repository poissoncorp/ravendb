import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import { Icon } from "components/common/Icon";
import React from "react";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EncryptionUnavailableMessage() {
    return (
        <LicenseRestrictedMessage>
            <StudioTrans
                ns="databases"
                i18nKey="createDatabase.encryptionUnavailable"
                components={{
                    strong: <strong className="text-info nobr" />,
                    icon: <Icon icon="storage" addon="encryption" margin="m-0" />,
                }}
            />
        </LicenseRestrictedMessage>
    );
}
